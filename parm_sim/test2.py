# -*- coding: utf-8 -*-
"""
Created on Wed May 20 00:06:16 2020

@author: YXG0RZH
"""


 # library for Microsoft Outlook access




def test():
    import datetime
    import openpyxl
    import os
    import os.path
    import pandas as pd
    import math
    import shutil
    today = datetime.datetime.today().strftime('%m-%d-%Y')  
    b =int((datetime.date.today()- datetime. date(2021, 2, 1)).days)/7
    i= 1+ math.floor(b)
    URL = r"\\at2a5\vol4\DEPTS\TRAFFIC\PROJECTS\SCD\Supply Chain Analytics\Projects\BULK Min Procedure (DO NOT DELETE - Nirjhar Raina)\Investment Report\Bulk investment reports\BULK INVESTMENT REPORT 5-11-2020 (FW15).xlsx"
    new_URL = os.path.join(r"\\at2a5\vol4\DEPTS\TRAFFIC\PROJECTS\SCD\Supply Chain Analytics\Projects\BULK Min Procedure (DO NOT DELETE - Nirjhar Raina)\Investment Report\Bulk investment reports\BULK INVESTMENT REPORT "+ today + " FW(" + str(i) + ").xlsx")
    shutil.copy(URL, new_URL)
    new_data = pd.read_csv(r"\\at2a5\vol4\DEPTS\TRAFFIC\PROJECTS\SCD\Supply Chain Analytics\Projects\BULK Min Procedure (DO NOT DELETE - Nirjhar Raina)\Gary_test\MON_investment_report.csv")
    dic = {"027":27,"022":22,"021":21,"030":30,"024":24}
    new_data.DEPT = new_data.DEPT.replace(dic)
    excel = pd.read_excel(new_URL,"INVESTMENT REPORT")
    book = openpyxl.load_workbook(new_URL)
    writer = pd.ExcelWriter(new_URL, engine='openpyxl')
    writer.book = book
    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
    a = new_data.copy()
    a.columns = excel.columns 
    a.to_excel(writer, "INVESTMENT REPORT",index=False)
    writer.save()
    writer.close()
