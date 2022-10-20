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


def ext_products():
    try:
        con = stg_connection.start()
        if con == -1:
            raise Exception(f"The database type {stg_connection.type} is not valid")
        elif con == -2:
            raise Exception("Error trying to connect to essgdbstaging")
        products_dict = {
            "prod_id": [],
            "prod_name": [],
            "prod_desc": [],
            "prod_category": [],
            "prod_category_id": [],
            "prod_category_desc": [],
            "prod_weight_class": [],
            "supplier_id": [],
            "prod_status": [],
            "prod_list_price": [],
            "prod_min_price": [],
        }
        #Lee .CSV
        products_csv = pd.read_csv(config.get(cvsName, "PRODUCTS_PATH"))
        #Procesa .CSV
         
        if not products_csv.empty:
            for id, pro_name, pro_desc, pro_cat, pro_cat_id, pro_cat_desc, pro_w_class, supp_id, pro_status, pro_list, pro_min, in zip(
                products_csv["PROD_ID"],
                products_csv["PROD_NAME"],
                products_csv["PROD_DESC"],
                products_csv["PROD_CATEGORY"],
                products_csv["PROD_CATEGORY_ID"],
                products_csv["PROD_CATEGORY_DESC"],
                products_csv["PROD_WEIGHT_CLASS"],
                products_csv["SUPPLIER_ID"],
                products_csv["PROD_STATUS"],
                products_csv["PROD_LIST_PRICE"],
                products_csv["PROD_MIN_PRICE"],
            ):
                products_dict["prod_id"].append(id)
                products_dict["prod_name"].append(pro_name)
                products_dict["prod_desc"].append(pro_desc)
                products_dict["prod_category"].append(pro_cat)
                products_dict["prod_category_id"].append(pro_cat_id)
                products_dict["prod_category_desc"].append(pro_cat_desc)
                products_dict["prod_weight_class"].append(pro_w_class)
                products_dict["supplier_id"].append(supp_id)
                products_dict["prod_status"].append(pro_status)
                products_dict["prod_list_price"].append(pro_list)
                products_dict["prod_min_price"].append(pro_min)

        if products_dict["prod_id"]:
            con.connect().execute("TRUNCATE TABLE products")
            
            df_channels = pd.DataFrame(products_dict)
            df_channels.to_sql("products", con, if_exists="append", index=False)
         
            con.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
