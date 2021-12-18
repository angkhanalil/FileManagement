from flask import Flask, Blueprint, json,jsonify,request
from flask_sqlalchemy import SQLAlchemy 
from sqlalchemy import create_engine ,text
import pandas as pd 
import pyodbc,pymssql
import sqlalchemy as sql
from sqlalchemy.sql.expression import false
ImportEmployee = Blueprint('Import_Employee', __name__) 

import urllib
#'PORT=1433;' \mssql+pymssql://COM1644\MSSQLEXPRESS/WORKWEAR?driver{SQL Server Native Client 10.0}

server = 'admin:admin1644@COM1644\MSSQLEXPRESS/UNIFORM?driver{SQL Server Native Client 10.0}' 
engine = sql.create_engine('mssql+pymssql://{}'.format(server))

@ImportEmployee.route('/hello')
def hello():
    return "Hello company"

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

@ImportEmployee.route('/import_employee',methods=['POST'])
def import_employee():
    if  request.method == 'POST':
        #get parameters 
        file = request.files['file']
        companyid = request.form["companyid"]
        #read file 
        data_emp_excel = pd.read_excel(file, sheet_name='Employee')
        df_emp_excel = pd.DataFrame(data_emp_excel)
        #check specila charactor
        df_emp_excel = df_emp_excel.rename(columns={'รหัสพนักงาน':'EMPLOYEE_NUMBER'}) #rename columns
        df_emp_excel['EMPLOYEE_NUMBER'] = df_emp_excel['EMPLOYEE_NUMBER'].astype(str)
        df_emp_excel['EMPLOYEE_NUMBER'] = df_emp_excel['EMPLOYEE_NUMBER'].str.strip().replace('[`,\',#,@,&,?, ]','',regex=True)
        print("### size ###")
        print(df_emp_excel.shape[0])

        df_empno_excel = pd.DataFrame(df_emp_excel['EMPLOYEE_NUMBER']) 
        df_empno_excel['EMPLOYEE_NUMBER'] = df_empno_excel['EMPLOYEE_NUMBER'].astype(str)
        print(df_emp_excel)

        sql = "SELECT *  FROM  WW_MAS_EMPLOYEE  where COMPANY_ID='"+companyid+"' "\
         " and FLAG_DEL_EMP='0' and EMP_STATUS='A' "
        res_emp = pd.read_sql_query(sql, engine)     
        df_res_emp = pd.DataFrame(res_emp) 
        print("*********************** data from sql *******************")
        print(df_res_emp)

        df_merge_emp = pd.merge(df_empno_excel['EMPLOYEE_NUMBER'] , df_res_emp['EMPLOYEE_NUMBER'],on=['EMPLOYEE_NUMBER'],how='inner')
        print("--------df_merge_emp----------")
        print(df_merge_emp)
        #duplicate employee = True
        df_empno_excel = df_empno_excel.append(df_merge_emp)
        df_empno_excel['Duplicated'] = df_empno_excel.duplicated(keep=False) # keep=False marks the duplicated row with a True
        print(df_empno_excel)

        print("-----------df_empno_excel del dup----------------")        
        df_empno_excel_dup = df_empno_excel[df_empno_excel['Duplicated']]  #.loc[df_empno_excel['Duplicated'] == "True"].loc[df_empno_excel.index.duplicated(), :]
        df_empno_excel_dup = df_empno_excel_dup.drop_duplicates(subset=['EMPLOYEE_NUMBER'])
        print("## dup size ##")
        print(df_empno_excel_dup.shape[0])

        # new employee
        df_new_employee = df_empno_excel[~df_empno_excel['Duplicated']]  # selects only rows which are not duplicated.

        print("****************df_new_employee.size********************")
        print(df_new_employee.shape[0])

        #- --------------- update employee -----------------------------#
        #else
        #update user
        if df_empno_excel_dup.size != 0:
            print(df_empno_excel_dup['EMPLOYEE_NUMBER'].tolist())
            df = df_emp_excel.loc[df_emp_excel['EMPLOYEE_NUMBER'].isin(df_empno_excel_dup['EMPLOYEE_NUMBER'].tolist())]
            print(df)
            #df2 = pd.merge(df_emp_excel , df_res_emp,on=['EMPLOYEE_NUMBER'],how='right')
            #print(df2)


        #---------------- insert new employee -----------------------------#
        if df_new_employee.size != 0:
            #insert new employee
            print(df_new_employee['EMPLOYEE_NUMBER'].tolist())
            #print(df_emp_excel)
            df = df_emp_excel.loc[df_emp_excel['EMPLOYEE_NUMBER'].isin(df_new_employee['EMPLOYEE_NUMBER'].tolist())]
            print(df) 
            df_to_table = pd.DataFrame(df['EMPLOYEE_NUMBER']) #df['EMPLOYEE_NUMBER']  #
            df_to_table['NAME_TH'] = df['Name']
            df_to_table['COMPANY_ID']   = companyid
            col = ['COMPANY_ID','EMPLOYEE_NUMBER','NAME_TH']
            df_to_table = df_to_table[col]
            print(df_to_table)
            #df_to_table.to_sql('WW_MAS_EMPLOYEE',con=engine,if_exists='append',index=False)      
            

    return jsonify(
        totalemployee=df_emp_excel.shape[0],
        newemployee=df_new_employee.shape[0],
        existsemployee=df_empno_excel_dup.shape[0]
    )

@ImportEmployee.route('/import_emp_product',methods=['POST'])
def import_emp_product():
    if  request.method == 'POST':
        #get parameters 
        file = request.files['file']
        companyid = request.form["companyid"]
        #read file 
        data_emp_excel = pd.read_excel(file, sheet_name='Employee_products_set')
        df_emp_excel = pd.DataFrame(data_emp_excel)

        

    return ""
