select a.*, b.*
from `analytics-ahs-owned-thd.{REPORTING_DATASET}.HDE_FALLOFF_JOB_LVL` as a
left join `analytics-ahs-owned-thd.{REPORTING_DATASET}.APPOINTMENTS_DETAILS` as b
  on b.LDLEAD = a.JOB
order by a.JOB
