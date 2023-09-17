import os
import logging
import requests
import psycopg2
import pandas as pd
from dotenv import load_dotenv

##loading the env variables into the session using the os and python-dotenv modules

load_dotenv()

#API_KEY         =   os.getenv("API_KEY")
#API_HOST        =   os.getenv("API_HOST")
#LEAGUE_ID       =   os.getenv("LEAGUE_ID")
#SEASON          =   os.getenv("SEASON")
#DB_NAME         =   os.getenv("DB_NAME")
#DB_USERNAME     =   os.getenv("DB_USERNAME")
#DB_PASSWORD     =   os.getenv("DB_PASSWORD")
#DB_HOST         =   os.getenv("DB_HOST")
#DB_PORT         =   os.getenv("DB_PORT")

DB_NAME="pipeline_1"
DB_USERNAME="postgres"
DB_PASSWORD="ifeoluwa"
DB_HOST="localhost"
DB_PORT="5432"

postgres_connection = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USERNAME,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

# Get a cursor from the database
cur = postgres_connection.cursor()