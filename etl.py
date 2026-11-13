
import os
import json
import sqlite3
import pandas as pd
import requests
from dotenv import load_dotenv

# Load API key
load_dotenv()
API_KEY = os.getenv("OMDB_API_KEY")

DB_FILE = "movies.db"
CACHE_FILE = "omdb_cache.json"


# ---------------- EXTRACT ----------------

def extract_data():
    movies = pd.read_csv("data/movies.csv")
    ratings = pd.read_csv("data/ratings.csv")

    return movies, ratings


# ---------------- OMDB API ----------------

def get_movie_details(title, year, cache):

    key = f"{title}_{year}"

    # Check cache first
    if key in cache:
        return cache[key]

    try:
        url = "https://www.omdbapi.com/"

        params = {
            "apikey": API_KEY,
            "t": title,
            "y": year
        }

        response = requests.get(url, params=params, timeout=10)
        data = response.json()

        if data.get("Response") == "True":
            cache[key] = data
            return data

    except requests.RequestException:
        print("API request failed:", title)

    return {}


# ---------------- TRANSFORM ----------------

def transform_movies(movies, ratings):

    # Extract release year from title
    movies["release_year"] = pd.to_numeric(
        movies["title"].str.extract(r"\((\d{4})\)$")[0],
        errors="coerce"
    )

    # Remove year from title
    movies["title"] = movies["title"].str.replace(
        r"\s\(\d{4}\)$", "", regex=True
    )

    # Convert titles like Matrix, The to The Matrix
    movies["title"] = movies["title"].str.replace(
        r",\s(The|A|An)$", "", regex=True
    )

    movies["decade"] = (movies["release_year"] // 10) * 10

    # Fetch OMDb details
    cache = {}

    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as file:
            cache = json.load(file)

    details = []

    for _, row in movies.iterrows():

        year = row["release_year"]

        if pd.notna(year):
            year = int(year)

        data = get_movie_details(row["title"], year, cache)

        details.append({
            "imdb_id": data.get("imdbID"),
            "director": data.get("Director"),
            "plot": data.get("Plot"),
            "box_office": data.get("BoxOffice")
        })

    # Save API cache
    with open(CACHE_FILE, "w") as file:
        json.dump(cache, file)

    details_df = pd.DataFrame(details)

    movies = pd.concat(
        [movies.reset_index(drop=True), details_df],
        axis=1
    )

    # Clean missing API values
    movies = movies.replace("N/A", None)

    # Convert box office into numeric values
    movies["box_office"] = pd.to_numeric(
        movies["box_office"].str.replace(r"\D", "", regex=True),
        errors="coerce"
    )

    # Identify movies found in OMDb
    movies["omdb_found"] = movies["imdb_id"].notna().astype(int)

    # Separate genres
    movie_genres = movies[["movieId", "genres"]].copy()

    movie_genres["genre"] = movie_genres["genres"].str.split("|")

    movie_genres = movie_genres.explode("genre")

    movie_genres = movie_genres[
        movie_genres["genre"] != "(no genres listed)"
    ]

    movie_genres = movie_genres[["movieId", "genre"]].drop_duplicates()

    # Clean ratings
    ratings = ratings.drop_duplicates(
        subset=["userId", "movieId"]
    )

    ratings = ratings.dropna(subset=["rating"])

    ratings["rated_at"] = pd.to_datetime(
        ratings["timestamp"], unit="s"
    )

    return movies, movie_genres, ratings


# ---------------- LOAD ----------------

def load_data(movies, movie_genres, ratings):

    conn = sqlite3.connect(DB_FILE)

    with open("schema.sql", "r") as file:
        conn.executescript(file.read())

    with conn:

        # Insert movies
        for _, row in movies.iterrows():

            conn.execute("""
                INSERT INTO movies
                (movie_id, title, release_year, decade, imdb_id,
                director, plot, box_office, omdb_found)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)

                ON CONFLICT(movie_id) DO UPDATE SET
                title=excluded.title,
                release_year=excluded.release_year,
                decade=excluded.decade,
                imdb_id=excluded.imdb_id,
                director=excluded.director,
                plot=excluded.plot,
                box_office=excluded.box_office,
                omdb_found=excluded.omdb_found
            """, (
                row["movieId"],
                row["title"],
                row["release_year"],
                row["decade"],
                row["imdb_id"],
                row["director"],
                row["plot"],
                row["box_office"],
                row["omdb_found"]
            ))

        # Insert genres
        for genre in movie_genres["genre"].unique():

            conn.execute(
                "INSERT OR IGNORE INTO genres (name) VALUES (?)",
                (genre,)
            )

        # Get genre IDs
        genre_ids = dict(
            conn.execute("SELECT name, genre_id FROM genres")
        )

        # Insert movie genres
        for _, row in movie_genres.iterrows():

            conn.execute("""
                INSERT OR IGNORE INTO movie_genres
                VALUES (?, ?)
            """, (
                row["movieId"],
                genre_ids[row["genre"]]
            ))

        # Insert ratings
        for _, row in ratings.iterrows():

            conn.execute("""
                INSERT OR IGNORE INTO ratings
                VALUES (?, ?, ?, ?)
            """, (
                row["userId"],
                row["movieId"],
                row["rating"],
                row["rated_at"].strftime("%Y-%m-%d %H:%M:%S")
            ))

    # Display table counts
    for table in ["movies", "genres", "movie_genres", "ratings"]:

        count = conn.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]

        print(table, ":", count)

    conn.close()


# ---------------- MAIN PIPELINE ----------------

def main():

    movies, ratings = extract_data()

    movies, movie_genres, ratings = transform_movies(
        movies, ratings
    )

    load_data(movies, movie_genres, ratings)

    print("ETL Pipeline completed successfully!")


if __name__ == "__main__":
    main()
