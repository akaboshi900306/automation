select current_datetime('America/New_York') as DATA_REFRESH_DATETIME
     , cal.FSCL_YR_WK_KEY_VAL
     , loc.HDE_DIVISION_NM as DIVISION
     , loc.HDE_REGION_NM as REGION
     , loc.HDE_BRANCH_NM as BRANCH
     , sum(TY_JOB_COUNT) as JOB_COUNT
     , sum(TY_INSTALLED_REVENUE_AMT) as IR
from `analytics-views-thd.SERVICES.HDIS_SCORECARD` as x
left join `pr-edw-views-thd.SHARED.CAL_PRD_HIER_FD` as cal_current
  on cal_current.cal_dt = current_date('America/New_York')
inner join `pr-edw-views-thd.SHARED.CAL_PRD_HIER_FD` as cal
  on cal.cal_dt = x.CAL_DT
  and cal.FSCL_WK_BGN_DT = cal_current.PREV_FWK_BGN_DT
left join `pr-edw-views-thd.SVCS_MYSERVICESDRILL.LOC_HIER_OPS_MERCH` as loc
  on loc.LOC_NBR = x.STR_NBR
where x.program_group = 'HDE'
and x.SFI_FLG = ''
group by 1,2,3,4,5
order by 1,2,3,4,5
