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


def ext_channels():
    try:
        con = stg_connection.start()
        if con == -1:
            raise Exception(f"The database type {stg_connection.type} is not valid")
        elif con == -2:
            raise Exception("Error trying to connect to essgdbstaging")
        cha_dict = {
            "channel_id": [],
            "channel_desc": [],
            "channel_class": [],
            "channel_class_id": [],
        }
        #Lee .CSV
        channel_csv = pd.read_csv(config.get(cvsName, "CHANNELS_PATH"))
        #Procesa .CSV
         
        if not channel_csv.empty:
            for id, des, cla, cla_id \
                in zip(
                channel_csv["CHANNEL_ID"],
                channel_csv["CHANNEL_DESC"],
                channel_csv["CHANNEL_CLASS"],
                channel_csv["CHANNEL_CLASS_ID"],
            ):
                cha_dict["channel_id"].append(id)
                cha_dict["channel_desc"].append(des)
                cha_dict["channel_class"].append(cla)
                cha_dict["channel_class_id"].append(cla_id)
        if cha_dict["channel_id"]:
            con.connect().execute("TRUNCATE TABLE channels")
            
            df_channels = pd.DataFrame(cha_dict)
            df_channels.to_sql("channels", con, if_exists="append", index=False)
         
            con.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
