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


def ext_promotions():
    try:
        con = stg_connection.start()
        if con == -1:
            raise Exception(f"The database type {stg_connection.type} is not valid")
        elif con == -2:
            raise Exception("Error trying to connect to essgdbstaging")
        promo_dict = {
            "promo_id": [],
            "promo_name": [],
            "promo_cost": [],
            "promo_begin_date": [],
            "promo_end_date": [],
        }
        #Lee .CSV
        promo_csv = pd.read_csv(config.get(cvsName, "PROMOTIONS_PATH"))
        #Procesa .CSV
         
        if not promo_csv.empty:
            for (id, prom_name, prom_cost, prom_begin, prom_end) in zip(
                promo_csv["PROMO_ID"],
                promo_csv["PROMO_NAME"],
                promo_csv["PROMO_COST"],
                promo_csv["PROMO_BEGIN_DATE"],
                promo_csv["PROMO_END_DATE"],
            ):
                promo_dict["promo_id"].append(id)
                promo_dict["promo_name"].append(prom_name)
                promo_dict["promo_cost"].append(prom_cost)
                promo_dict["promo_begin_date"].append(prom_begin)
                promo_dict["promo_end_date"].append(prom_end)

        if promo_dict["promo_id"]:
            con.connect().execute("TRUNCATE TABLE promotions")
            
            df_channels = pd.DataFrame(promo_dict)
            df_channels.to_sql("promotions", con, if_exists="append", index=False)
         
            con.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
