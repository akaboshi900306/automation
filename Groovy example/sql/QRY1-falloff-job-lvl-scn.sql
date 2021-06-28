select a.*
from `analytics-ahs-owned-thd.BSC.HDE_FALLOFF_JOB_LVL` as a
inner join `pr-edw-views-thd.SVCS_MYSERVICESDRILL.LOC_HIER_OPS_MERCH` as b
  on a.STR_NBR = b.LOC_NBR
  and b.HDE_SCN_FLG = 'Y'
