import pandas as pd
import os

# ---------- Ratings functions ----------
def extract_ratings(input_path='./Data/extracted/ratings.csv'):
    df = pd.read_csv(input_path)
    return df

def clean_ratings(df_ratings):
    df_ratings = df_ratings.drop(columns=['timestamp'])
    return df_ratings

# ---------- Movies functions ----------
def extract_movies(input_path='./Data/extracted/movies.csv'):
    df = pd.read_csv(input_path, encoding='latin-1')
    return df

#cummulates the genres into a single column
def clean_movies(df):
    df = df.drop(columns=['video_release_date','IMDb_URL'])
    genre_list = []
    for _, row in df.iterrows():
        movie_genres = []
        for col in df.columns[3:]:  # genre columns start after movieId, title, release_date
            if row[col] == 1:
                movie_genres.append(col)
        genre_list.append(movie_genres)
    df['genres'] = genre_list
    df = df.drop(columns=df.columns[3:-1])
    return df

#adds the averager rating and number of ratings 
def enrich_movies(df_movies, df_ratings):
    # Ensure movie_id is int
    df_movies["movie_id"] = df_movies["movie_id"].astype(int)
    df_ratings["movie_id"] = df_ratings["movie_id"].astype(int)
    # Compute avg_rating and num_ratings
    movie_stats = df_ratings.groupby("movie_id")["rating"].agg(
        avg_rating="mean",
        num_ratings="count"
    ).reset_index()
    df_movies = df_movies.merge(movie_stats, on="movie_id", how="left")
    return df_movies

#-------------user_flair Function--------------------
#user flair data creation
def create_user_flair(df_ratings):
    user_stats = df_ratings.groupby("user_id").agg(
        num_movies_rated=("movie_id", "count"),
        avg_rating_given=("rating", "mean"),
        movies_rated=("movie_id", lambda x: list(x))
    ).reset_index()
    return user_stats

# ---------- Save function ---------- 
def save_df(df, path): 
    df.to_csv(path, index=False)
    
# ---------- Main ----------
def transform():
    # Ratings
    df_ratings = extract_ratings()
    df_ratings_cleaned = clean_ratings(df_ratings)
    save_df(df_ratings_cleaned,"./Data/transformed/transformed_ratings.csv")
    # Movies
    df_movies = extract_movies()
    df_movies_cleaned = clean_movies(df_movies)
    df_movies_enriched = enrich_movies(df_movies_cleaned, df_ratings_cleaned)
    save_df(df_movies_enriched, "./Data/transformed/transformed_movies.csv")    
    #user flairs
    df_user= create_user_flair(df_ratings_cleaned)
    save_df(df_user,"./Data/transformed/user_flair.csv")
    print("Transformation completed!")
