import requests
import pandas as pd
import logging
from sqlalchemy import create_engine

# class CovidScraper():
#     def __init__(self):
#         pass

#     def get_data(self):
#         response = requests.get(self.url)
#         result = response.json()['data']['content']
#         logging.info("GET DATA FROM API COMPLETED")
#         df = pd.json_normalize(result)
#         logging.info("DATA FROM API TO DATAFRAME READY")
#         return df

# DB_USER="mysql"
# DB_PASSWORD="mysql"
# DB_HOST="localhost"
# DB_PORT="3307"
# DB_NAME="mysql"
# CONNECTION_STRING = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# engine = create_engine(CONNECTION_STRING)

# def connect_mysql(user, password, host, db, port):
#         engine = create_engine("mysql+mysqlconnector://{}:{}@{}:{}/{}".format(
#             user, password, host, port, db
#         ))
#         return engine

response = requests.get("https://dsg-api.com/clients/{client_name}/soccer/get_tables?type={type}&id={id}&client={client_name}&authkey={client_authkey}", auth=("User", "Password"))
result = response.json()
# result = response.json()['data']['content']
df = pd.json_normalize(result)
print(df)

# engine = connect_mysql(
#         host = "localhost",
#         user = "mysql",
#         password= "mysql",
#         db= "mysql",
#         port="3307"
#     )
# df.to_sql('province_table',con=engine,if_exists='replace', index=False)

