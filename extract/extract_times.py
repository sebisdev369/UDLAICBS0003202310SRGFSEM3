#Import de libs
from util import db_connection
import pandas as pd
from datetime import datetime

import configparser

import traceback

#Uso de Parser para .properties
config = configparser.ConfigParser()
config.read(".properties")
config.get("DatabaseCredentials", "DB_TYPE")
databaseName = "DatabaseCredentials"
#Credenciales BD
stg_connection = db_connection.Db_Connection(
    config.get(databaseName, "DB_TYPE"),
    config.get(databaseName, "DB_HOST"),
    config.get(databaseName, "DB_PORT"),
    config.get(databaseName, "DB_USER"),
    config.get(databaseName, "DB_PWD"),
    config.get(databaseName, "STG_NAME"),
)
#Ruta archivos .CSV
cvsName = "CSVFiles"


def ext_times():
    try:
        con = stg_connection.start()
        if con == -1:
            raise Exception(f"The database type {stg_connection.type} is not valid")
        elif con == -2:
            raise Exception("Error trying to connect to essgdbstaging")
        times_dict = {
            "time_id": [],
            "day_name": [],
            "day_number_in_week": [],
            "day_number_in_month": [],
            "calendar_week_number": [],
            "calendar_month_number": [],
            "calendar_month_desc": [],
            "end_of_cal_month": [],
            "calendar_quarter_desc": [],
            "calendar_year": [],
        }
        #Lee .CSV
        times_csv = pd.read_csv(config.get(cvsName, "TIMES_PATH"))
        #Procesa .CSV
         
        if not times_csv.empty:
            for id,day_n,day_n_week,day_n_month,cal_week_n,cal_month_n,cal_month_des,cal_end,cal_qua_desc,cal_year, in zip(
                times_csv["TIME_ID"],
                times_csv["DAY_NAME"],
                times_csv["DAY_NUMBER_IN_WEEK"],
                times_csv["DAY_NUMBER_IN_MONTH"],
                times_csv["CALENDAR_WEEK_NUMBER"],
                times_csv["CALENDAR_MONTH_NUMBER"],
                times_csv["CALENDAR_MONTH_DESC"],
                times_csv["END_OF_CAL_MONTH"],
                times_csv["CALENDAR_QUARTER_DESC"],
                times_csv["CALENDAR_YEAR"],
            ):
                times_dict["time_id"].append(id)
                times_dict["day_name"].append(day_n)
                times_dict["day_number_in_week"].append(day_n_week)
                times_dict["day_number_in_month"].append(day_n_month)
                times_dict["calendar_week_number"].append(cal_week_n)
                times_dict["calendar_month_number"].append(cal_month_n)
                times_dict["calendar_month_desc"].append(cal_month_des)
                times_dict["end_of_cal_month"].append(cal_end)
                times_dict["calendar_quarter_desc"].append(cal_qua_desc)
                times_dict["calendar_year"].append(cal_year)

        if times_dict["time_id"]:
            con.connect().execute("TRUNCATE TABLE times")
            
            df_channels = pd.DataFrame(times_dict)
            df_channels.to_sql("times", con, if_exists="append", index=False)
         
            con.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
