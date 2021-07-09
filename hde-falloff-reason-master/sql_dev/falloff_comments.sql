with li as
(
    SELECT
        row_number() over (partition by liLead order by liAuditDateTime asc) as ord,
        row_number() over (partition by liLead order by liAuditDateTime desc) as rev_ord,
        liAuditDateTime,
        liLead,
        liDate2,
        LDFUPDATE,
    FROM
        `analytics-ahs-owned-thd.Wp0c42_RptRemodel.v_TblLeadInstallationDates`
        left join `pr-edw-views-thd.HOMESERVICES.TBLLEADS` on lilead = ldlead
    where liType = 'INS'
        and LDADATE >= date_sub(current_date('UTC'),interval 728 day)
),

FSL as
(
    SELECT
        wo.LMS_LEADID__C,
        timestamp(sah.CREATEDDATE,'UCT') AS REASON_DATETIME,
        substr(NEWVALUE,24) as REASON
    FROM
        `pr-edw-views-thd.HOMESERVICES_CONF.SERVICEAPPOINTMENTHISTORY` sah
        left join `pr-edw-views-thd.HOMESERVICES_CONF.SERVICEAPPOINTMENT` sa on sa.ID = sah.SERVICEAPPOINTMENTID
        left join `pr-edw-views-thd.HOMESERVICES_CONF.WORKORDER` wo on wo.id = sa.PARENTRECORDID
    WHERE 1 = 1
        AND NEWVALUE LIKE 'Reschedule Requested -%'
        AND FIELD = 'Provider_Issues__c'
)


select
    li.*,
    case when li.lidate2 is null then 'Unscheduled' else 'Rescheduled' END  as TYPE,
    ifnull(aud.FSCL_YR_WK_KEY_VAL >= pje.FSCL_YR_WK_KEY_VAL and aud.FSCL_YR_WK_KEY_VAL >= paud.FSCL_YR_WK_KEY_VAL,false ) as FALLOFF_FLAG,
    Max(
        ifnull(REASON,
            if(cmcomment like '%by TOA%',
                case when cmcomment like 'Service appointment scheduled by % failed%' then 'Service appointment scheduling failed'
                    when cmcomment like '%Installer appointment scheduled by %' then 'Installer appointment scheduled'
                    when cmcomment like '%Service appointment scheduled by %' then 'Service appointment scheduled'
                    when CMCOMMENT like '%Unschedule Reason code -  Other%' then 'Unschedule Reason code -  Other'
                    when CMCOMMENT like '%Reschedule Reason code -  Other%' then 'Reschedule Reason code -  Other'
                    when CMCOMMENT like '%schedule Reason code%'
                        then SUBSTR(CMCOMMENT
                                ,strpos(CMCOMMENT,'schedule Reason code')-2
                                ,if(strpos(CMCOMMENT,'Scheduling Comments')>0,
                                    strpos(CMCOMMENT,'Scheduling Comments') + 2 - strpos(CMCOMMENT,'schedule Reason code'),
                                    length(cmcomment) +2 - strpos(CMCOMMENT,'schedule Reason code'))
                                )
                    when CMCOMMENT like '%Override Reason code%'
                        then SUBSTR(CMCOMMENT
                                ,strpos(CMCOMMENT,'Override Reason code')
                                ,if(strpos(CMCOMMENT,'Scheduling Comments')>0,
                                    strpos(CMCOMMENT,'Scheduling Comments') - strpos(CMCOMMENT,'Override Reason code'),
                                    length(cmcomment) - strpos(CMCOMMENT,'Override Reason code'))
                                )
                    when CMCOMMENT like '%appointment %scheduled by %' then 'No reason found'
                    else cmcomment
                END,'No reason found'
                )
            )
        ) as SELECTED_REASON,
    MAX(lower(cmcomment) like '%weather%'
        or lower(cmcomment) like '% rain%'
        or lower(cmcomment) like '% snow%'
        or lower(cmcomment) like '%storm%'
    ) AS WEATHER_FLAG,

    STRING_AGG(
                if(cmcomment like '%by TOA%' or cmcomment like '%by FSL%',
                    if(strpos(CMCOMMENT,'Scheduling Comments')>0,
                        concat(CMDATETIME,' ',
                                SUBSTR(CMCOMMENT,strpos(CMCOMMENT,'Scheduling Comments'))
                                ),''),
                    CMCOMMENT)
                order by CMDATETIME asc)
    AS COMMENTS
from
    li
    #Get prior insatll date
    left join li as li2 on li.lilead = li2.lilead and li.ord = li2.ord+1
    left join `pr-edw-views-thd.SHARED.CAL_PRD_HIER_FD` pje on pje.CAL_DT = li2.lidate2
    left join `pr-edw-views-thd.SHARED.CAL_PRD_HIER_FD` paud on paud.CAL_DT = date(li2.liAuditDateTime)
    left join `pr-edw-views-thd.SHARED.CAL_PRD_HIER_FD` aud on aud.CAL_DT = date(li.liAuditDateTime)
    left join `pr-edw-views-thd.SHARED.CAL_PRD_HIER_FD` fup on fup.CAL_DT = li.LDFUPDATE

    left join `analytics-ahs-owned-thd.Wp0c42_RptRemodel.v_TBLLEADCOMMENTS`
            on cmlead = li.lilead and abs(datetime_diff(CMDATETIME, li.liAuditDateTime, MINUTE)) < 5
    LEFT JOIN FSL ON cast(FSL.LMS_LEADID__C as int64) = li.lilead AND abs(datetime_diff(FSL.REASON_DATETIME, timestamp(li.liAuditDateTime,'America/New_York'), HOUR)) < 5
where 1=1
    and li.ord > 1
    # and li.lilead = 10942170
group by 1,2,3,4,5,6,7,8
# order by liAuditDateTime desc

union distinct

SELECT
    LI.*,
    'Aged'  as TYPE,
    TRUE as FALLOFF_FLAG,
    'No reason found' as SELECTED_REASON,
    MAX(lower(cmcomment) like '%weather%'
        or lower(cmcomment) like '% rain%'
        or lower(cmcomment) like '% snow%'
        or lower(cmcomment) like '%storm%'
    ) AS WEATHER_FLAG,
    STRING_AGG(concat(CMDATETIME, CMCOMMENT) order by CMDATETIME asc),
FROM
    LI
    left join `pr-edw-views-thd.SHARED.CAL_PRD_HIER_FD` je on je.CAL_DT = lidate2
    left join `pr-edw-views-thd.SHARED.CAL_PRD_HIER_FD` fup on fup.CAL_DT = ifnull(LI.LDFUPDATE,current_date('America/New_York'))
    left join `analytics-ahs-owned-thd.Wp0c42_RptRemodel.v_TBLLEADCOMMENTS`
            on cmlead = lilead and DATE(CMDATE) BETWEEN DATE(lidate2) AND LDFUPDATE
WHERE rev_ord = 1 and fup.FSCL_YR_WK_KEY_VAL > je.FSCL_YR_WK_KEY_VAL
group by 1,2,3,4,5,6#,fup.FSCL_YR_WK_KEY_VAL, je.FSCL_YR_WK_KEY_VAL
order by lilead, ord

# LIMIT 100
