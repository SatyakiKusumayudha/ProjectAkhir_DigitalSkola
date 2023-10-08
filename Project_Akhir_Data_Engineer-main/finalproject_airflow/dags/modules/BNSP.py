import requests
import pandas as pd
import logging
import numpy as np

# class CovidScraper():
#     def __init__(self,url):
#         self.url = url

#     def get_data(self):
#         response = requests.get(self.url)
#         result = response.json()['data']['content']
#         logging.info("GET DATA FROM API COMPLETED")
#         df = pd.json_normalize(result)
#         logging.info("DATA FROM API TO DATAFRAME READY")
#         return df

response = requests.get("https://e-serkom-ng.co.id/exam_app/api/de_exam/pegawai")
result = response.json()['data']
df = pd.json_normalize(result)
print(df)
