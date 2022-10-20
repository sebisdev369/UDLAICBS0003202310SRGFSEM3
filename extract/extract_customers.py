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


def ext_customers():
    try:
        con = stg_connection.start()
        if con == -1:
            raise Exception(f"The database type {stg_connection.type} is not valid")
        elif con == -2:
            raise Exception("Error trying to connect to essgdbstaging")
        customers_dict = {
            "cust_id": [],
            "cust_first_name": [],
            "cust_last_name": [],
            "cust_gender": [],
            "cust_year_of_birth": [],
            "cust_marital_status": [],
            "cust_street_address": [],
            "cust_postal_code": [],
            "cust_city": [],
            "cust_state_province": [],
            "country_id": [],
            "cust_main_phone_number": [],
            "cust_income_level": [],
            "cust_credit_limit": [],
            "cust_email": [],
        }
        #Lee .CSV
        customers_csv = pd.read_csv(config.get(cvsName, "CUSTOMERS_PATH"))
        #Procesa .CSV
        
        if not customers_csv.empty:
            for id,first_name,last_name,gender,year_birth,m_status,street,postal,city,state_province,country_id,phone,income,credit,email, in zip(
                customers_csv["CUST_ID"],
                customers_csv["CUST_FIRST_NAME"],
                customers_csv["CUST_LAST_NAME"],
                customers_csv["CUST_GENDER"],
                customers_csv["CUST_YEAR_OF_BIRTH"],
                customers_csv["CUST_MARITAL_STATUS"],
                customers_csv["CUST_STREET_ADDRESS"],
                customers_csv["CUST_POSTAL_CODE"],
                customers_csv["CUST_CITY"],
                customers_csv["CUST_STATE_PROVINCE"],
                customers_csv["COUNTRY_ID"],
                customers_csv["CUST_MAIN_PHONE_NUMBER"],
                customers_csv["CUST_INCOME_LEVEL"],
                customers_csv["CUST_CREDIT_LIMIT"],
                customers_csv["CUST_EMAIL"],
            ):
                customers_dict["cust_id"].append(id)
                customers_dict["cust_first_name"].append(first_name)
                customers_dict["cust_last_name"].append(last_name)
                customers_dict["cust_gender"].append(gender)
                customers_dict["cust_year_of_birth"].append(year_birth)
                customers_dict["cust_marital_status"].append(m_status)
                customers_dict["cust_street_address"].append(street)
                customers_dict["cust_postal_code"].append(postal)
                customers_dict["cust_city"].append(city)
                customers_dict["cust_state_province"].append(state_province)
                customers_dict["country_id"].append(country_id)
                customers_dict["cust_main_phone_number"].append(phone)
                customers_dict["cust_income_level"].append(income)
                customers_dict["cust_credit_limit"].append(credit)
                customers_dict["cust_email"].append(email)

        if customers_dict["cust_id"]:
            con.connect().execute("TRUNCATE TABLE customers")
            
            df_channels = pd.DataFrame(customers_dict)
            df_channels.to_sql("customers", con, if_exists="append", index=False)
         
            con.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
