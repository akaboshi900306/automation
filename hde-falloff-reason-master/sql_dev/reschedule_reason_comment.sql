select
    case when cmcomment like 'Service appointment scheduled by TOA failed%' then 'Service appointment scheduled by TOA failed'
        when cmcomment like '%Installer appointment scheduled by TOA%' then 'Installer appointment scheduled by TOA'
        when cmcomment like '%Service appointment scheduled by TOA%' then 'Service appointment scheduled by TOA'
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
        when CMCOMMENT like '%appointment Re-scheduled by TOA%' then 'No Re-schedule reason given'
        else cmcomment
    END as Comment,
    lower(cmcomment) like '%weather%'
        or lower(cmcomment) like '% rain%'
        or lower(cmcomment) like '% snow%'
        or lower(cmcomment) like '%storm%'
    as weather_flag,
    # strpos(CMCOMMENT,'Reschedule Reason code'),
    # strpos(CMCOMMENT,'Scheduling Comments'),
    # CMCOMMENT,
    count(*) as count,
    min(cmdate) as start_date
from
    `analytics-ahs-owned-thd.Wp0c42_RptRemodel.v_TBLLEADCOMMENTS`
where
    cmcomment like '%by TOA%'
    and CMCOMMENT not like '%Measure appointment %scheduled by TOA%'
    and CMCOMMENT not like 'Comments from HDE Workflow:  Email Status:  Submitted%'
    # and CMCOMMENT like '%Re-scheduled%'
    # and (CMCOMMENT like '%Reschedule Reason code%' or CMCOMMENT like '%Override Reason code%')
group by 1,2#,3
order by 3 desc
# limit 100
