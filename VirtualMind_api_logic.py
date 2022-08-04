import pandas as pd
import sqlite3 as sq
import datetime
import flask
from flask import request, jsonify

app = flask.Flask(__name__)
app.config["DEBUG"] = True
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///log_data.db"

error_template = {"status": "error", "message": ""}

def isValidTS(start_ts, end_ts):
    try:
        start_ts_dt = datetime.datetime.strptime(start_ts, '%Y-%m-%d %H:%M:%S')
        end_ts_dt = datetime.datetime.strptime(end_ts, '%Y-%m-%d %H:%M:%S')
        
        if start_ts_dt <= end_ts_dt:
            return True
        else:
            return False
    
    except ValueError:
        return False

# one time table load function. if table does not exist then it will be executed. please update click_log.csv path
def create_load_click_log():
    connection = sq.connect('log_data.db')
    curs = connection.cursor()
    curs.execute("SELECT name FROM sqlite_master WHERE type='table' and name = 'click_log';")
    if curs.fetchone() == None:
        create_tbl_sql = """create table click_log(
        ID NUMERIC(34), 
        TIMESTAMP TIMESTAMP, 
        TYPE VARCHAR(50), 
        CAMPAIGN NUMERIC(10), 
        BANNER NUMERIC(10),
        CONTENT_UNIT NUMERIC(10),
        NETWORK INT2, 
        BROWSER INT2, 
        OPERATING_SYSTEM INT2, 
        COUNTRY INT, 
        STATE INT, 
        CITY INT
        )"""
        
        curs.execute(create_tbl_sql)
        
        file_path = 'C:/Users/apapap/Downloads/click_log.csv'
        pd_log_csv = pd.read_csv(file_path)
        
        # convert timestamp epoch to 'yyyy-mm-dd hh:mm:ss'
        pd_log_csv['timestamp'] = pd.to_datetime(pd_log_csv['timestamp'], unit='s')
        
        # Load csv data into sqlite table
        pd_log_csv.to_sql('click_log', connection, if_exists='append', index=False)
        connection.close()


def campaign_click_count(camp, start_date, end_date):
    try:
        connection = sq.connect('log_data.db')
        curs = connection.cursor()
        
        # optional period filter. period filter will be blank unless user provides start and end date values
        period_filter = "" if start_date == None and end_date == None else " AND TIMESTAMP BETWEEN '" + start_date + "' AND '" + end_date + "'"
        search_sql = """select count(*) as camp_click_count from click_log
                        where campaign = :campaign""" + period_filter
                        
        print(search_sql)
        
        #passing campaign as parameter to sqlite sql
        click_count = curs.execute(search_sql, {"campaign": camp}).fetchone()[0]
        connection.close()
        success_output = {"status": "success",
        "data": {"campaign" : camp, "count" : click_count},
        "message": "operation completed successfully"}

        return success_output
        
    except Exception as err:
        # display any sqlite excpetion as message
        error_template["message"] = str(err)
        return error_template


def param_validation(camp_id, start_date, end_date):

    #validate campaign
    if camp_id != None  and camp_id.isdigit():
        click_camp_id = int(camp_id)
        # validate start and end date
        if start_date != None and end_date != None:
            if isValidTS(start_date, end_date):
                return 2
            else:
                error_template["message"] = "bad input : optional parameters start date and end date must be in 'yyyy-MM-dd hh:mm:ss' format & start_date must be less than or equal to end_date"
                return error_template
        elif start_date == None and end_date == None:
            return 1
        else:
            error_template["message"] = "bad input : optional parameters start date and end date both are required"
            return error_template
    else:
        error_template["message"] = "bad input : campaign parameter is a must parameter & it should be integer"
        return error_template


@app.route('/', methods=['GET'])
def home():
    return '''<h1>click_log count</h1>
<p>A prototype API for Virtual Minds.</p>'''

@app.route('/api/campaignInfo', methods=['GET'])
def api_id():
    # check input parameters
    if 'campaign' in request.args:
        camp_id = request.args.get('campaign')
        start_date = request.args.get('start_date', None)
        end_date = request.args.get('end_date', None)
        print(camp_id, start_date, end_date)
    else:
        # campaign number is must for this api. if it is missing then display error message
        error_template["message"] = "Campaign field not provided. Please specify campaign id"
        return error_template
    
    create_load_click_log()
    param_check = param_validation(camp_id, start_date, end_date)
    print(param_check)
    if param_check == 1:
        output = campaign_click_count(int(camp_id), None, None)
    elif param_check == 2:
        output = campaign_click_count(int(camp_id), start_date, end_date)
    else:
        output = param_check
    return output

app.run()

