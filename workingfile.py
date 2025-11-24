import mysql.connector
import pandas as pd

# connect to database
source_conn = mysql.connector.connect(
    host="ccscloud.dlsu.edu.ph",
    user="root",
    port="60822",
    password="password", # walang password tho
    database="FactOrder"
)




engine = create_engine(
    "mysql+mysqlconnector://megan:Megan%401234@34.142.244.237:3306/stadvdb" #still to change
)

with engine.connect() as connection:
    print("Now connected to Cloud Data Warehouse!")
