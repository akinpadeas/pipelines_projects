import os
import psycopg2
import pandas as pd 
from PIL import Image
import streamlit as st
import plotly.express as px
from dotenv import load_dotenv


# Load the environment variables 
load_dotenv()

API_KEY         =   os.getenv("API_KEY")
API_HOST        =   os.getenv("API_HOST")
LEAGUE_ID       =   os.getenv("LEAGUE_ID")
SEASON          =   os.getenv("SEASON")
DB_NAME         =   os.getenv("DB_NAME")
DB_USERNAME     =   os.getenv("DB_USERNAME")
DB_PASSWORD     =   os.getenv("DB_PASSWORD")
DB_HOST         =   os.getenv("DB_HOST")
DB_PORT         =   os.getenv("DB_PORT")

#DB_NAME="pipeline_1"
#DB_USERNAME="postgres"
#DB_PASSWORD="ifeoluwa"
#DB_HOST="localhost"
#DB_PORT="5432"
#
postgres_connection = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USERNAME,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)


# Fetch the Premier League data from Postgres
get_premier_league_standings_sql_query = """
    SELECT 
        position
        ,team
        ,games_played
        ,wins
        ,draws
        ,losses
        ,goals_for
        ,goals_against
        ,goal_difference
        ,points
    FROM 
        public.premier_league_standings_vw
    ORDER BY 
        position;
"""


# Read football data into dataframe
final_standings_df = pd.read_sql(get_premier_league_standings_sql_query, postgres_connection)


# Remove the index displayed
final_standings_df.set_index('position', inplace=True)


# Close database connection
postgres_connection.close()

#######################################################
#######################################################
#### Display the data in streamlit
# Set the page configuration of the app
st.set_page_config(
    page_title   =  "Premier League Standings 2023/24",
    page_icon    =  "⚽",
    layout       =  "wide"
)

#adding premier logo to the top right sideof the web app
# Read image into app
#prem_league_logo_filepath  =  "/assets/premier_league_logo.png"
prem_league_logo_filepath  = "/Users/sammy4code/pipelines_projects/assets/premier_league_logo.png"
prem_league_logo_image     =  Image.open(prem_league_logo_filepath)


# Create columns for the layout and display the image through the 2nd one
col1, col2 = st.columns([4, 1])
col2.image(prem_league_logo_image)

#title
st.title("⚽🏆 Premier League Table Standings 2023/24 ⚽🏆")

# Display instructions
st.sidebar.title('Instructions 📖')
st.sidebar.write("""
The table showcases the current Premier League standings for the 2023/24 season. Toggle the visualization options to gain deeper insights!
""")

show_visualization = st.sidebar.radio('Would you like to view the standings as a visualization too?', ('No', 'Yes'))
fig_specification  = px.bar(final_standings_df, 
                        x           =   'team', 
                        y           =   'points', 
                        title       =   'Premier League Standings 2023/24', 
                        labels      =   {'points':'Points', 'team':'Team', 'goals_for': 'Goals Scored', 'goals_against': 'Goals Conceded', 'goal_difference':'Goal Difference'},
                        color       =   'team',
                        height      =   600,
                        hover_data  =   ['goals_for', 'goals_against', 'goal_difference']
)

if show_visualization == 'Yes':
    st.table(final_standings_df)
    st.write("")
    fig = fig_specification 
    st.plotly_chart(fig, use_container_width=True)
else:
    st.table(final_standings_df)
