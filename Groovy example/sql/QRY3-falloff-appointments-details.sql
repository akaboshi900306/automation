SELECT
    LD.LDLEAD,
    LD.LDLOC,
    LD.LDFUPDATE,
    W.ID AS WORKORDER_ID,
    W.CREATEDDATE AS WORKORDER_CREATEDDATE,
    APP.ID AS APPT_ID,
    APP.CREATEDDATE AS APPT_CREATEDDATE,
    APP.STATUS AS APPT_STATUS,
    APP.CATEGORY__C,
    APP.SCHEDSTARTTIME,
    APP.SCHEDENDTIME,
    APP.ACTUALSTARTTIME,
    APP.ACTUALENDTIME
FROM `pr-edw-views-thd.HOMESERVICES_CONF.TBLLEADS` LD
LEFT JOIN `pr-edw-views-thd.HOMESERVICES_CONF.WORKORDER` W ON CAST(LD.LDLEAD AS STRING) = W.LMS_LEADID__C
LEFT JOIN `pr-edw-views-thd.HOMESERVICES_CONF.SERVICEAPPOINTMENT` app ON w.ID = app.PARENTRECORDID
LEFT JOIN `SALESFORCE.RECORDTYPE` R ON app.RECORDTYPEID = R.ID
WHERE R.NAME = 'THD HDE INSTALL'
ORDER BY w.CREATEDDATE ASC, app.CREATEDDATE ASC
