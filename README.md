#  upload_file.py 
## 1.use openpyxl to manipulate excel sheet
## 2. split df into different excels
``` 
while count >0 :
    num = min(count,1999)
    df = df2.iloc[0:num]
    path= r"\\at2a5\vol4\DEPTS\TRAFFIC\PROJECTS\SCD\Supply Chain Analytics\Projects\Gary Gao\OverrideUpload_allocation"
    df.to_csv(os.path.join(path, 'RDC_OverrideUpload_' + d. strftime('%Y-%m-%d') + '_' + str(i) + '.csv'),index=False)
    count = count - num
    df2 = df2.iloc[-count:]
    i=i+1
```
## 3. use of df.iterrow() to get sum() over (partition by)
```
def build_truck(g):
    a = 0
    g.sort_values(['STORE_RANK','VIRT_EXPCTD_ALLOC_QTY',"WOS"], ascending=[True, False, True], inplace=True)
    for i, row in g.iterrows():
        a = a+ row["VIRT_EXPCTD_ALLOC_QTY"]
        if a  <= row["QTY_TO_ALLOC"]:
            g.loc[i,"included_qty"] = row["VIRT_EXPCTD_ALLOC_QTY"]
        if a >= row["QTY_TO_ALLOC"]:
            g.loc[i,"included_qty"] = row["QTY_TO_ALLOC"] - (a- row["VIRT_EXPCTD_ALLOC_QTY"])
    return g
```

# error_file.py
## 1.use df.iterrow combine with the contents in the query
```
for index, row in df.iterrows():
    sql = """update  `analytics-supplychain-thd.DISTRO.FP_PLAY_DLY`
    set EXECUTE = "N"
    where CAL_DT = DATE(CURRENT_TIMESTAMP(), 'US/Eastern')
    and RDC_NBR=""" + str(row["RDC"]) + """ and ASN_NUMBER= """ + "'"+str(row["ASN_NBR"])+"'"
    
    sql = read_gbq(sql, project_id='analytics-supplychain-thd')
 ```
 
 # parm_sim.py
 ## 1. use win32com to send email !!!!
