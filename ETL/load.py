import pandas as pd
from sqlalchemy import create_engine, text
import os

from dotenv import load_dotenv

def get_env_variables():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")
    return user, password, host, port, db 

def load_to_mysql():
    load_dotenv()
    user, password, host, port, db = get_env_variables()
    # Connect without specifying database
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}")

    # Create database if not exists
    with engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db}"))

    # Now connect specifically to the target DB
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")

    # Load CSVs
    df_ratings = pd.read_csv("./Data/transformed/transformed_ratings.csv")
    df_ratings.to_sql("ratings", engine, if_exists="replace", index=False)

    df_movies = pd.read_csv("./Data/transformed/transformed_movies.csv")
    df_movies.to_sql("movies", engine, if_exists="replace", index=False)

    df_user = pd.read_csv("./Data/transformed/user_flair.csv")
    df_user.to_sql("user_flair", engine, if_exists="replace", index=False)

    print("✅ Database created (if not exists) and data loaded!")
