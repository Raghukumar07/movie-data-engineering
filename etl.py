"""Movie ETL pipeline: MovieLens CSVs + OMDb API -> SQLite database.

1) Extract   - read the CSV files, ask the OMDb API for extra movie details
2) Transform - clean titles, genres, ratings and API values (with pandas)
3) Load      - save everything in SQLite (safe to run many times)
"""
import json
import os
import re
import sqlite3

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()  # reads OMDB_API_KEY from the .env file
API_KEY = os.getenv("OMDB_API_KEY")
MAX_CALLS = int(os.getenv("OMDB_MAX_CALLS", "950"))  # free key allows ~1000 calls per day
CACHE_FILE = "omdb_cache.json"  # remembers every API answer, so reruns cost nothing
DB_FILE = "movies.db"


# ---------- small helper functions ----------
def split_title(raw):
    """'Matrix, The (1999)' -> ('The Matrix', 1999)"""
    match = re.match(r"^(.*)\s\((\d{4})\)\s*$", raw)
    title, year = (match.group(1).strip(), int(match.group(2))) if match else (raw.strip(), None)
    match = re.match(r"^(.*), (The|A|An)$", title)
    if match:
        title = f"{match.group(2)} {match.group(1)}"
    return title, year


def call_omdb(title, year):
    """Ask OMDb about one movie. Returns its JSON answer, or None if the call failed."""
    params = {"apikey": API_KEY, "t": title, "y": year}  # year=None is skipped automatically
    try:
        return requests.get("http://www.omdbapi.com/", params=params, timeout=10).json()
    except (requests.RequestException, ValueError):
        return None


def get_api_data(movies, cache):
    """Return {movie_id: OMDb answer} for the movies OMDb knows.
    Looks in the cache first and only calls the API for new movies."""
    answers = {}
    calls_left = MAX_CALLS
    for row in movies.itertuples():
        year = None if pd.isna(row.release_year) else int(row.release_year)
        key = f"{row.title}|{year}"
        if key not in cache and calls_left > 0:
            calls_left -= 1
            data = call_omdb(row.title, year)
            error = (data or {}).get("Error", "")
            if "API key" in error:  # wrong or missing key: stop right away
                raise SystemExit(f"OMDb rejected the API key ({error}). Check OMDB_API_KEY in .env")
            if error == "Request limit reached!":  # daily limit used up: no more calls today
                calls_left = 0
            elif data and (data.get("Response") == "True" or error == "Movie not found!"):
                cache[key] = data  # save real answers and genuine "not found"; never save errors
        data = cache.get(key)
        if data and data.get("Response") == "True":
            answers[int(row.movieId)] = data
    return answers


def to_rows(df):
    """DataFrame -> list of tuples for SQLite. Missing values (NaN) become None (NULL)."""
    return list(df.astype(object).where(df.notna(), None).itertuples(index=False, name=None))


# ---------- the pipeline ----------
def main():
    # ===== EXTRACT =====
    movies = pd.read_csv("data/movies.csv")
    ratings = pd.read_csv("data/ratings.csv")

    # ===== TRANSFORM: movies =====
    parts = movies["title"].apply(split_title)  # one (title, year) pair per movie
    movies["title"] = parts.str[0]
    movies["release_year"] = parts.str[1].astype("Int64")  # Int64 allows missing years
    movies["decade"] = movies["release_year"] // 10 * 10  # feature engineering: 1999 -> 1990

    # Ask the API about the most-rated movies first, because the daily limit is small
    n_ratings = movies["movieId"].map(ratings["movieId"].value_counts()).fillna(0)
    most_rated_first = movies.loc[(-n_ratings).sort_values(kind="stable").index]
    cache = json.load(open(CACHE_FILE)) if os.path.exists(CACHE_FILE) else {}
    try:
        answers = get_api_data(most_rated_first, cache)
    finally:
        json.dump(cache, open(CACHE_FILE, "w"))  # always save, even if the script stops early
    print(f"API data found for {len(answers)} of {len(movies)} movies "
        f"(run again tomorrow to fetch more)")

    # Enrich: add the API details as new columns (movies OMDb doesn't know get NULLs)
    columns = ["imdbID", "Director", "Plot", "BoxOffice"]
    details = pd.DataFrame.from_dict(answers, orient="index", columns=columns)
    details = details.mask(details == "N/A")  # OMDb writes "N/A" for missing data
    movies = movies.join(details, on="movieId").rename(
        columns={"imdbID": "imdb_id", "Director": "director", "Plot": "plot"})
    # "$191,796,233" -> 191796233
    movies["box_office"] = pd.to_numeric(
        movies["BoxOffice"].str.replace(r"\D", "", regex=True), errors="coerce").astype("Int64")
    movies["omdb_found"] = movies["movieId"].isin(list(answers)).astype(int)

    # ===== TRANSFORM: genres ("Action|Comedy" -> one row per genre) =====
    movie_genres = movies.assign(genre=movies["genres"].str.split("|")).explode("genre")
    movie_genres = movie_genres[movie_genres["genre"] != "(no genres listed)"]
    movie_genres = movie_genres[["movieId", "genre"]].drop_duplicates()

    # ===== TRANSFORM: ratings =====
    ratings = ratings.drop_duplicates(subset=["userId", "movieId"]).dropna(subset=["rating"])
    ratings = ratings[ratings["movieId"].isin(movies["movieId"])].copy()  # drop unknown movies
    ratings["rated_at"] = pd.to_datetime(ratings["timestamp"], unit="s").dt.strftime("%Y-%m-%d %H:%M:%S")

    # ===== LOAD =====
    conn = sqlite3.connect(DB_FILE)
    conn.executescript(open("schema.sql").read())  # creates the tables if they don't exist
    with conn:  # one transaction: everything is saved, or nothing is
        # Upsert: a new movie is inserted, an existing one is updated (no duplicates)
        conn.executemany("""
            INSERT INTO movies (movie_id, title, release_year, decade, imdb_id,
                                director, plot, box_office, omdb_found)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(movie_id) DO UPDATE SET
                title=excluded.title, release_year=excluded.release_year,
                decade=excluded.decade, imdb_id=excluded.imdb_id,
                director=excluded.director, plot=excluded.plot,
                box_office=excluded.box_office, omdb_found=excluded.omdb_found
        """, to_rows(movies[["movieId", "title", "release_year", "decade", "imdb_id",
                            "director", "plot", "box_office", "omdb_found"]]))

        # INSERT OR IGNORE skips rows that already exist, so reruns add no duplicates
        conn.executemany("INSERT OR IGNORE INTO genres (name) VALUES (?)",
                        [(g,) for g in movie_genres["genre"].unique()])
        genre_ids = dict(conn.execute("SELECT name, genre_id FROM genres"))
        conn.executemany("INSERT OR IGNORE INTO movie_genres VALUES (?, ?)",
                        [(m, genre_ids[g]) for m, g in to_rows(movie_genres)])
        conn.executemany("INSERT OR IGNORE INTO ratings VALUES (?, ?, ?, ?)",
                        to_rows(ratings[["userId", "movieId", "rating", "rated_at"]]))

    for table in ("movies", "genres", "movie_genres", "ratings"):
        print(table, conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], "rows")
    conn.close()


if __name__ == "__main__":
    main()