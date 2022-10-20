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


def ext_sales():
    try:
        con = stg_connection.start()
        if con == -1:
            raise Exception(f"The database type {stg_connection.type} is not valid")
        elif con == -2:
            raise Exception("Error trying to connect to essgdbstaging")
        sales_dict = {
            "prod_id": [],
            "cust_id": [],
            "time_id": [],
            "channel_id": [],
            "promo_id": [],
            "quantity_sold": [],
            "amount_sold": [],
        }
        #Lee .CSV
        sales_csv = pd.read_csv(config.get(cvsName, "SALES_PATH"))
        #Procesa .CSV
         
        if not sales_csv.empty:
            for id, cus_id, time_id, cha_id, prom_id, quant_sold, amt_sold in zip(
                sales_csv["PROD_ID"],
                sales_csv["CUST_ID"],
                sales_csv["TIME_ID"],
                sales_csv["CHANNEL_ID"],
                sales_csv["PROMO_ID"],
                sales_csv["QUANTITY_SOLD"],
                sales_csv["AMOUNT_SOLD"],
            ):
                sales_dict["prod_id"].append(id)
                sales_dict["cust_id"].append(cus_id)
                sales_dict["time_id"].append(time_id)
                sales_dict["channel_id"].append(cha_id)
                sales_dict["promo_id"].append(prom_id)
                sales_dict["quantity_sold"].append(quant_sold)
                sales_dict["amount_sold"].append(amt_sold)

        if sales_dict["prod_id"]:
            con.connect().execute("TRUNCATE TABLE sales")
            
            df_channels = pd.DataFrame(sales_dict)
            df_channels.to_sql("sales", con, if_exists="append", index=False)
         
            con.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
