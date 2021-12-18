from flask import Flask, Blueprint, json,jsonify,request
from flask_sqlalchemy import SQLAlchemy 
from sqlalchemy import create_engine ,text
import pandas as pd 
import pyodbc,pymssql
import sqlalchemy as sql
from sqlalchemy.sql.expression import false
Import = Blueprint('Import', __name__) 

import urllib
#'PORT=1433;' \mssql+pymssql://COM1644\MSSQLEXPRESS/WORKWEAR?driver{SQL Server Native Client 10.0}

server = 'admin:admin1644@COM1644\MSSQLEXPRESS/UNIFORM?driver{SQL Server Native Client 10.0}' 
engine = sql.create_engine('mssql+pymssql://{}'.format(server))
        
#************************************************************************************
#params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 10.0};SERVER=dagger;DATABASE=test;UID=user;PWD=password")
#engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)         
#param = 'DRIVER={ODBC Driver 13 for SQL Server};' \
#         'SERVER=COM1644\\MSSQLEXPRESS;' \
#         'DATABASE=UNIFORM;' \
#         'UID=sa;' \
#         'PWD=@com1644;' 
            
#params = urllib.parse.quote_plus(param)
#db = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
#************************************************************************************

@Import.route('/hello')
def hello():
    return "Hello company"

@Import.route('/import_employee0',methods=['POST'])
def import_employee0():
    if request.method == 'POST': #form-data
        file = request.files['file']
        companyid = request.form["companyid"]
       
        data = pd.read_excel(file, sheet_name='Employee')
        df = pd.DataFrame(data)
        df = df.rename(columns={'รหัสพนักงาน':'EMPLOYEE_NUMBER'})
        #print(companyid)
        #print(df.columns.tolist())
        #print(df)
        #print(df['รหัสพนักงาน'].values)
        dataframe_excel = pd.DataFrame(df['EMPLOYEE_NUMBER'])
        dataframe_excel['EMPLOYEE_NUMBER'] = dataframe_excel['EMPLOYEE_NUMBER'].astype(str)
        #print(dataframe_excel)
        sql = "SELECT EMPLOYEE_NUMBER  FROM [UNIFORM].[dbo].[WW_EMPLOYEE]  where COMPANY_ID='"+companyid+"' and FLAG_DEL_EMP='0' "
        dfsql = pd.read_sql_query(sql, engine) 
    
        datasql = pd.DataFrame(dfsql)
        #print(datasql)

        df_merge = pd.merge(dataframe_excel , datasql,on=['EMPLOYEE_NUMBER'],how='inner')
        #,on=['EMPLOYEE_NUMBER']
        dataframe_excel = dataframe_excel.append(df_merge)
        #duplicate employee = True
        dataframe_excel['Duplicated'] = dataframe_excel.duplicated(keep=False) # keep=False marks the duplicated row with a True
        # new employee
        df_final = dataframe_excel[~dataframe_excel['Duplicated']]  # selects only rows which are not duplicated.
        #del df_final['Duplicated'] # delete the indicator column
        print(df_final)
        dataframe_excel.to_excel('C:/Users/Administrator/Documents/emp.xlsx',index=False)
    return "data" #excel2json.convert_from_file(file)

#------------------------------------------------------
def update_employee():

    sql_update = "UPDATE WW_MAS_EMPLOYEE  " \
                " SET NAME_TH = WW_MAS_EMPLOYEE_TEMP_UPDATE.NAME_TH "\
                " FROM [WW_MAS_EMPLOYEE_TEMP_UPDATE] LEFT JOIN  WW_MAS_EMPLOYEE " \
                " ON WW_MAS_EMPLOYEE.EMPLOYEE_NUMBER = WW_MAS_EMPLOYEE_TEMP_UPDATE.EMPLOYEE_NUMBER COLLATE THAI_CI_AI  " \
                " AND WW_MAS_EMPLOYEE_TEMP_UPDATE.COMPANY_ID = WW_MAS_EMPLOYEE.COMPANY_ID COLLATE THAI_CI_AI  " \
                " WHERE WW_MAS_EMPLOYEE.FLAG_DEL_EMP = '0' AND WW_MAS_EMPLOYEE.EMP_STATUS = 'A' "

    with engine.begin() as conn:
        conn.execute(sql_update) 

    return 0


@Import.route('/import_employee',methods=['POST'])
def import_employee():

    if request.method == 'POST': #form-data
        #get parameters 
        file = request.files['file']
        companyid = request.form["companyid"]
        #read file 
        data_emp_excel = pd.read_excel(file, sheet_name='Employee')
        df_emp_excel = pd.DataFrame(data_emp_excel)
        #check specila charactor

        df_emp_excel = df_emp_excel.rename(columns={'รหัสพนักงาน':'EMPLOYEE_NUMBER'}) #rename columns
        df_emp_excel['EMPLOYEE_NUMBER'] = df_emp_excel['EMPLOYEE_NUMBER'].astype(str)
        df_emp_excel['EMPLOYEE_NUMBER'] = df_emp_excel['EMPLOYEE_NUMBER'].str.strip().replace('[`,\',#,@,&,?]','',regex=True)
        #df emp
        df_empno_excel = pd.DataFrame(df_emp_excel['EMPLOYEE_NUMBER']) 
        df_empno_excel['EMPLOYEE_NUMBER'] = df_empno_excel['EMPLOYEE_NUMBER'].astype(str)
        print(df_emp_excel)
        print("before check ")
        print(df_empno_excel)
        #df_empno_excel['EMPLOYEE_NUMBER'] = df_empno_excel.replace(df_empno_excel['EMPLOYEE_NUMBER'], regex=True)
        df_empno_excel['EMPLOYEE_NUMBER'] = df_empno_excel['EMPLOYEE_NUMBER'].str.strip().replace('[`,\',#,@,&,?]','',regex=True)
        
        #df.columns = df.columns.str.replace('[#,@,&]', '')
        print("after check ")
        print(df_empno_excel)
        #select emp from sql server
        sql = "SELECT EMPLOYEE_NUMBER  FROM  WW_MAS_EMPLOYEE  where COMPANY_ID='"+companyid+"' "\
         " and FLAG_DEL_EMP='0' and EMP_STATUS='A' "
        res_emp = pd.read_sql_query(sql, engine)     
        df_res_emp = pd.DataFrame(res_emp) 
        print("***********************df_res_emp*******************")
        print(df_res_emp)
        #merge empnumber 
        df_merge_emp = pd.merge(df_empno_excel , df_res_emp,on=['EMPLOYEE_NUMBER'],how='inner')
        print("--------df_merge_emp----------")
        print(df_merge_emp)
        df_empno_excel = df_empno_excel.append(df_merge_emp)
        #df_empno_excel = df_empno_excel.loc[df_empno_excel.index.duplicated(), :]
        print(df_empno_excel)
        #duplicate employee = True
        df_empno_excel['Duplicated'] = df_empno_excel.duplicated(keep=False) # keep=False marks the duplicated row with a True
        print(df_empno_excel)

        print("-----------df_empno_excel del dup----------------")        
        df_empno_excel_dup = df_empno_excel[df_empno_excel['Duplicated']]  #.loc[df_empno_excel['Duplicated'] == "True"].loc[df_empno_excel.index.duplicated(), :]
        print(df_empno_excel_dup)
        #df_empno_excel_new = df_empno_excel.loc[df_empno_excel['Duplicated'] == "False"]#.loc[df_empno_excel.index.duplicated(), :]
        #print(df_empno_excel_new)
        #merge เจอ แสดงว่าซ้ำ
        df_ex = df_emp_excel.loc[df_emp_excel['EMPLOYEE_NUMBER'].isin(df_merge_emp['EMPLOYEE_NUMBER'].tolist())]#.loc[df_empno_excel['Duplicated'] == "True"]#.loc[df_empno_excel.index.duplicated(), :]
        
        print("****************df_ex********************")
        print(df_ex)
        #export file excel exists user
        #df_ex.to_excel('C:/Users/Administrator/Documents/df_ex.xlsx',index=False)
        # new employee
        df_new_employee = df_empno_excel[~df_empno_excel['Duplicated']]  # selects only rows which are not duplicated.

        print("****************df_new_employee.size********************")
        print(df_new_employee)
       
        #- --------------- update employee -----------------------------#
        #else
        #update user
        if df_ex.size != 0:
            print("ds")
            sql = "SELECT * FROM  WW_MAS_EMPLOYEE  where COMPANY_ID='"+companyid+"' and FLAG_DEL_EMP='0' and EMP_STATUS='A' "
            res_emp_ex = pd.read_sql_query(sql, engine)  

            df_res_emp_dup = pd.DataFrame(res_emp_ex)
            df_res_emp_dup.loc[df_res_emp_dup['EMPLOYEE_NUMBER'].isin(df_ex['EMPLOYEE_NUMBER'].tolist())]
            print(df_res_emp_dup)

            df2 = pd.merge(df_emp_excel , df_res_emp_dup,on=['EMPLOYEE_NUMBER'],how='right')
            print(df2)

            col = ['EMPLOYEE_ID','COMPANY_ID','EMPLOYEE_NUMBER','NAME_TH' ]
            df_to_table_dup = pd.DataFrame(df2['EMPLOYEE_NUMBER']) #df['EMPLOYEE_NUMBER']  #[']
            df_to_table_dup['EMPLOYEE_ID'] = df2['EMPLOYEE_ID']#.astype(str)
            df_to_table_dup['NAME_TH'] = df2['Name']
            df_to_table_dup['COMPANY_ID']   = companyid 
            #df_to_table_dup['NAME_EN'] =  df2['Name En']
            #df_to_table_dup['BRANCH_NO'] = df2['branch']
            #df_to_table_dup['BRANCH_NAME'] = df2['branhcname']
            #df_to_table_dup['PHONE_NO'] = df2['tel']
            #df_to_table_dup['EMAIL'] = df2['email']
            #df_to_table_dup['IDCARD'] = df2['ID Card']
            #df_to_table_dup['GENDER'] = df2['GENDER']
            #df_to_table_dup['POSITION_NO'] = ''
            #df_to_table_dup['POSITION_NAME']=''
            #df_to_table_dup['EMP_STATUS'] = 'A' 
            #f_to_table_dup['CREATED_BY'] = '' 
            df_to_table_dup = df_to_table_dup[col]
            print("************df_to_table_dup**************")
            print(df_to_table_dup)
            df_to_table_dup.to_sql('WW_MAS_EMPLOYEE_TEMP_UPDATE',con=engine,if_exists='replace',index=False)
            #update_employee()
        #---------------- insert new employee -----------------------------#
        if df_new_employee.size != 0:
            #insert new employee
            print(df_new_employee['EMPLOYEE_NUMBER'].tolist())
            df = df_emp_excel.loc[df_emp_excel['EMPLOYEE_NUMBER'].isin(df_new_employee['EMPLOYEE_NUMBER'].tolist())]
            #print(df) 
            df_to_table = pd.DataFrame(df['EMPLOYEE_NUMBER']) #df['EMPLOYEE_NUMBER']  #
            df_to_table['NAME_TH'] = df['Name']
            df_to_table['COMPANY_ID']   = companyid
            col = ['COMPANY_ID','EMPLOYEE_NUMBER','NAME_TH']
            df_to_table = df_to_table[col]
            #print(df_to_table)
            df_to_table.to_sql('WW_MAS_EMPLOYEE',con=engine,if_exists='append',index=False)      

    return "" 


 
