from flask import Flask, json,jsonify,request,Blueprint
#from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine ,text
#import pyodbc,pymssql
from flask_marshmallow import Marshmallow
from flask_cors import CORS 
from blueprints.employee.Import_Employee import ImportEmployee
from blueprints.orders.Import_orders import ImportOrders
app = Flask(__name__)
CORS(app)
#app.config.from_object('config')
#engine = create_engine('mssql+pyodbc://COM1644\MSSQLEXPRESS/WORKWEAR?driver={SQL Server Native Client 10.0}?Trusted_Connection=yes')

#db = SQLAlchemy(app)
#engine = db.engine
#connection = engine.connect()
#serialize data marshmallo
ma = Marshmallow(app)

app.register_blueprint(ImportEmployee, url_prefix='/ImportEmployee')
app.register_blueprint(ImportOrders,url_prefix='/ImportOrders')

if __name__ == '__main__':
    app.run(debug=True) #False