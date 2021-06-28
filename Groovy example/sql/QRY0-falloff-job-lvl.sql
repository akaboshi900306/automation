select *
, IF(Status = 'Canceled', JOB_AMT, 0) as JOB_AMT_CANCELED
, IF(Status = 'Canceled', 1, 0) as JOB_COUNT_CANCELED

, IF(Status = 'Rescheduled', JOB_AMT, 0) as JOB_AMT_RESCHEDULED
, IF(Status = 'Rescheduled', 1, 0) as JOB_COUNT_RESCHEDULED

, IF(Status = 'Aged', JOB_AMT, 0) as JOB_AMT_AGED
, IF(Status = 'Aged', 1, 0) as JOB_COUNT_AGED

, IF(Status = 'Unscheduled', JOB_AMT, 0) as JOB_AMT_UNSCHEDULED
, IF(Status = 'Unscheduled', 1, 0) as JOB_COUNT_UNSCHEDULED

, IF(Status = 'Installed', JOB_AMT, 0) as JOB_AMT_INSTALLED
, IF(Status = 'Installed', 1, 0) as JOB_COUNT_INSTALLED

from (
  SELECT
    current_datetime('America/New_York') as DATA_REFRESH_DATETIME
    , cal_previous.FSCL_YR_WK_KEY_VAL
    , J.LDLEAD AS JOB
    , lpad(cast(J.ldlocation2 as string), 4, '0') as STR_NBR
    , J.LDSALESAMT AS JOB_AMT
    , case
      when j.LDCANDATE is not null then 'Canceled'
      when cal_installdate.fscl_wk_nbr != cal_jobenddate.fscl_wk_nbr then 'Rescheduled'
      when cal_installdate.fscl_wk_nbr = cal_jobenddate.fscl_wk_nbr and j.LDFUPDATE is null then 'Aged'
      when cal_installdate.fscl_wk_nbr is null then 'Unscheduled'
      when cal_jobenddate.fscl_wk_nbr = cal_installdate.fscl_wk_nbr then 'Installed'
    else 'Error' end as STATUS
    , concat(ISC.LCFNAME  ,' ', ISC.LCLNAME) as ISC_NAME
    , concat(ISM.LCFNAME  ,' ', ISM.LCLNAME) as ISM_NAME
    , concat(sc.SMLNAME , ' ', sc.SMFNAME) as SC_NAME
    , CAST(j.LDJOBENDDATE AS DATE) as NEW_SCHEDULED_JOB_END_DATE
    , cast(BOW.LDJOBENDDATE as DATE) as ORIG_SCHEDULED_JOB_END_DATE
    , CAST(J.LDFUPDATE AS DATE) as FUP_DATE
    , date_diff(
        coalesce(cast(j.LDJOBENDDATE as date), cast(j.LDFUPDATE as date), date_sub(current_date('America/New_York'), interval 1 day)),
        CAST(j.ldadate AS DATE)
        , day
      ) as JOB_AGE_DAYS
    , LMS_JOB_COMMENT_1
    , LMS_JOB_COMMENT_2
    , LMS_JOB_COMMENT_3
    , LMS_JOB_REASON_1
    , LMS_JOB_REASON_2
    , LMS_JOB_REASON_3
  FROM `analytics-ahs-owned-thd.Wp0c42_RptRemodel.BOW_Scheduled_Hist` as BOW
  left join `pr-edw-views-thd.HOMESERVICES.TBLLEADS` as J
    ON cast(BOW.LDLEAD as int64) = J.LDLEAD
  /* Fiscal Calendar */
  -- Filter to fiscal week of BOW_Scheduled_Hist (by Job End Date)
  -- If report is run on Monday, will look at BOW Snapshot 7 days ago, and look at job information thru previous day
  -- cal_jobenddate: Calendar attributes based on Scheduled Job End Date
  inner join `pr-edw-views-thd.SVCS_MYSERVICESDRILL.CAL_PRD_HIER` as cal_jobenddate
    on cal_jobenddate.cal_dt = CAST(BOW.LDJOBENDDATE AS DATE)
  left join `pr-edw-views-thd.SHARED.CAL_PRD_HIER_FD` as cal_current
    on cal_current.cal_dt = current_date('America/New_York')
  inner join `pr-edw-views-thd.SHARED.FSCL_WK_HIER_FD` as cal_previous
    on cal_previous.FSCL_WK_BGN_DT = cal_current.PREV_FWK_BGN_DT
    and cal_previous.FSCL_YR_WK_KEY_VAL = cal_jobenddate.FSCL_YR_WK_KEY_VAL
  -- cal_installdate: Calendar attributes based on (Installed jobs: FUP Date...Uninstalled jobs: Scheduled Job End Date)
  LEFT JOIN  `pr-edw-views-thd.SVCS_MYSERVICESDRILL.CAL_PRD_HIER` as cal_installdate
    on cal_installdate.cal_dt = ifnull(CAST(j.LDJOBENDDATE AS DATE), CAST(J.LDFUPDATE AS DATE))
  -- Get ISM
  left JOIN `analytics-ahs-owned-thd.Wp0c42_RptRemodel.TBLJOBASSIGNMENTS` as IM
    ON J.LDLEAD = cast(IM.jaLead as int64)
    AND IM.jaPosition = 'IM'
  left JOIN `analytics-ahs-owned-thd.Wp0c42_RptRemodel.TBLUSERMASTER` as ISM
    ON IM.jaUserNO = ISM.LCCOODNO
  -- Get ISC ("PC" is "Operations Center Coordinator")
  left JOIN `analytics-ahs-owned-thd.Wp0c42_RptRemodel.TBLJOBASSIGNMENTS` as PC
    ON J.LDLEAD = cast(PC.jaLead as int64)
    AND PC.jaPosition = 'PC'
  left JOIN `analytics-ahs-owned-thd.Wp0c42_RptRemodel.TBLUSERMASTER` as ISC
    ON PC.jaUserNO = ISC.LCCOODNO
  -- Get SC
  left JOIN `analytics-ahs-owned-thd.Wp0c42_RptRemodel.TBLSALESMANMASTER` as SC
    ON J.LDSALES1 = cast(SC.SMSALESNO as int64)

  /* Comments & Reasons */
  --Get Last Three Comments (at lead level)
  Left join (
    select
      cmlead
      , MAX(IF(ComNum = 1, Comment,null)) as LMS_JOB_COMMENT_1
      , MAX(IF(ComNum = 2, Comment,null)) as LMS_JOB_COMMENT_2
      , MAX(IF(ComNum = 3, Comment,null)) as LMS_JOB_COMMENT_3
    from (
      select
        concat(REPLACE(cmcomment, '\\n', '') ,' - ' , SUBSTR(cmdate,0,10),' - ', LCFNAME, ' ' , LCLNAME) as Comment
        , cmlead
        , row_number() over (partition by cmlead order by cmkey desc) as ComNum
      From `analytics-ahs-owned-thd.Wp0c42_RptRemodel.TBLLEADCOMMENTS`
      inner join `analytics-ahs-owned-thd.Wp0c42_RptRemodel.TBLUSERMASTER`
        ON cmUser = LCCOODNO
      where cast(cmlead as int64) in (
        select ldlead from `analytics-ahs-owned-thd.Wp0c42_RptRemodel.BOW_Scheduled_Hist`
      )
    )
    group by cmlead
  ) as p1
    on cast(p1.cmlead as int64) = j.ldlead

  --Get last 3 Reasons (at lead level)
  Left join (
    select
      lrlead
      , MAX(IF(ReasonNum = 1, Job_Issue,null)) as LMS_JOB_REASON_1
      , MAX(IF(ReasonNum = 2, Job_Issue,null)) as LMS_JOB_REASON_2
      , MAX(IF(ReasonNum = 3, Job_Issue,null)) as LMS_JOB_REASON_3
    from (
      select
        concat(rtrim(rmdesc) , ': ' , rtrim(srdesc) ,' - ' , SUBSTR(lrdate,0,10)  ,' - ',LCFNAME,' ' , LCLNAME) as Job_Issue
        , lrlead
        , row_number() over (partition by lrlead order by lrkey desc) as ReasonNum
      From `analytics-ahs-owned-thd.Wp0c42_RptRemodel.TBLLEADREASONS`
  		inner join (
        select min(lrkey) as minkey
        from `analytics-ahs-owned-thd.Wp0c42_RptRemodel.TBLLEADREASONS`
        group by lrlead, LRReason, LRSubReason
      ) as mk
        on minkey = LRKey
  		left join `analytics-ahs-owned-thd.Wp0c42_RptRemodel.TBLREASONMASTER`
        on LRReason = rmreason
  		left join `analytics-ahs-owned-thd.Wp0c42_RptRemodel.TBLSUBREASONMASTER`
        on LRSubReason = srsubreason
        and srreason = lrreason
  		left join `analytics-ahs-owned-thd.Wp0c42_RptRemodel.TBLUSERMASTER`
        ON lrUser = LCCOODNO
  		where cast(lrlead as int64) in (
        select ldlead from `analytics-ahs-owned-thd.Wp0c42_RptRemodel.BOW_Scheduled_Hist`
      )
    )
    group by lrlead
  ) lr
    on cast(lrlead as int64) = j.ldlead
)
