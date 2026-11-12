"""Movie ETL pipeline: MovieLens CSVs + OMDb API -> SQLite."""
import json
import logging
import os
import re
import sqlite3

import pandas as pd
import requests

try:  # optional: load OMDB_API_KEY from a .env file if python-dotenv is installed
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

API_KEY = os.getenv("OMDB_API_KEY")
CACHE_FILE = os.getenv("OMDB_CACHE", "omdb_cache.json")
DB_FILE = os.getenv("DB_FILE", "movies.db")
DATA_DIR = os.getenv("DATA_DIR", "data")
MAX_CALLS = int(os.getenv("OMDB_MAX_CALLS", "950"))  # free key allows ~1000/day
calls_made = 0
limit_reached = False  # set when OMDb reports the daily limit


class ApiLimit(Exception):
    """Raised when we should stop calling the API for this run."""

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("etl")


# ---------- helpers ----------
def parse_title(raw):
    """'Matrix, The (1999)' -> ('The Matrix', 1999)."""
    m = re.match(r"^(.*)\s\((\d{4})\)\s*$", raw)
    title, year = (m.group(1).strip(), int(m.group(2))) if m else (raw.strip(), None)
    m2 = re.match(r"^(.*), (The|A|An)$", title)
    if m2:
        title = f"{m2.group(2)} {m2.group(1)}"
    return title, year


def na_to_none(v):
    return None if v in (None, "", "N/A") else v


def clean_box_office(v):
    if not v or v == "N/A":
        return None
    digits = re.sub(r"[^\d]", "", v)
    return int(digits) if digits else None


def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)


def fetch_omdb(title, year, cache):
    global calls_made, limit_reached
    key = f"{title}|{year}"
    if key in cache:
        return cache[key]
    if limit_reached or calls_made >= MAX_CALLS:
        raise ApiLimit  # no new API calls, cached data is still used
    calls_made += 1
    params = {"apikey": API_KEY, "t": title}
    if year:
        params["y"] = year
    try:
        data = requests.get("http://www.omdbapi.com/", params=params, timeout=10).json()
    except (requests.RequestException, ValueError):
        return None  # transient error: not cached, retried next run
    if data.get("Error") == "Request limit reached!":
        limit_reached = True
        raise ApiLimit
    cache[key] = data
    return data


# ---------- extract / transform ----------
def extract():
    movies = pd.read_csv(os.path.join(DATA_DIR, "movies.csv"))
    ratings = pd.read_csv(os.path.join(DATA_DIR, "ratings.csv"))
    return movies, ratings


def transform(movies, ratings, cache):
    parsed = movies["title"].apply(parse_title)
    movies = movies.copy()
    movies["clean_title"] = parsed.str[0]
    movies["release_year"] = parsed.str[1].astype("Int64")
    movies["decade"] = (movies["release_year"] // 10 * 10).astype("Int64")

    # Most-rated movies first, so a limited API quota covers the movies that matter most
    counts = ratings["movieId"].value_counts()
    order = sorted(movies["movieId"], key=lambda m: -counts.get(m, 0))
    by_id = movies.set_index("movieId")
    api, found, limit_hit = {}, 0, False
    for mid in order:
        r = by_id.loc[mid]
        year = None if pd.isna(r["release_year"]) else int(r["release_year"])
        try:
            d = fetch_omdb(r["clean_title"], year, cache)  # cache first, API only if allowed
        except ApiLimit:
            d = None
            if not limit_hit:
                limit_hit = True
                log.warning("API limit reached. Movies without cached data are loaded without API data; rerun later.")
        if d and d.get("Response") == "True":
            found += 1
            api[mid] = (na_to_none(d.get("imdbID")), na_to_none(d.get("Director")),
                        na_to_none(d.get("Plot")), clean_box_office(d.get("BoxOffice")), 1)
        else:
            api[mid] = (None, None, None, None, 0)
    log.info("API data found for %d of %d movies", found, len(movies))
    movies[["imdb_id", "director", "plot", "box_office", "omdb_found"]] = pd.DataFrame(
        movies["movieId"].map(api).tolist(), index=movies.index)

    # genres
    g = movies[["movieId", "genres"]].copy()
    g["genre"] = g["genres"].str.split("|")
    g = g.explode("genre")
    g = g[g["genre"].notna() & (g["genre"] != "(no genres listed)")]
    movie_genres = g[["movieId", "genre"]].drop_duplicates()

    # ratings
    ratings = ratings.drop_duplicates(subset=["userId", "movieId"]).dropna(subset=["rating"])
    ratings = ratings[ratings["movieId"].isin(movies["movieId"])].copy()
    ratings["rated_at"] = pd.to_datetime(ratings["timestamp"], unit="s").dt.strftime("%Y-%m-%d %H:%M:%S")
    return movies, movie_genres, ratings


# ---------- load ----------
def nn(v):
    return None if pd.isna(v) else v


def load(conn, movies, movie_genres, ratings):
    with conn:
        conn.executemany("""
            INSERT INTO movies (movie_id,title,release_year,decade,imdb_id,director,plot,box_office,omdb_found)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(movie_id) DO UPDATE SET
            title=excluded.title, release_year=excluded.release_year, decade=excluded.decade,
            imdb_id=excluded.imdb_id, director=excluded.director, plot=excluded.plot,
            box_office=excluded.box_office, omdb_found=excluded.omdb_found
        """, [(int(r.movieId), r.clean_title,
            None if pd.isna(r.release_year) else int(r.release_year),
            None if pd.isna(r.decade) else int(r.decade),
            nn(r.imdb_id), nn(r.director), nn(r.plot),
            None if pd.isna(r.box_office) else int(r.box_office), int(r.omdb_found))
            for r in movies.itertuples()])
        conn.executemany("INSERT OR IGNORE INTO genres(name) VALUES (?)",
                        [(g,) for g in movie_genres["genre"].unique()])
        ids = dict(conn.execute("SELECT name, genre_id FROM genres"))
        conn.executemany("INSERT OR IGNORE INTO movie_genres VALUES (?,?)",
                        [(int(r.movieId), ids[r.genre]) for r in movie_genres.itertuples()])
        conn.executemany("INSERT OR IGNORE INTO ratings VALUES (?,?,?,?)",
                        [(int(r.userId), int(r.movieId), float(r.rating), r.rated_at)
                        for r in ratings.itertuples()])


def main():
    conn = sqlite3.connect(DB_FILE)
    with open("schema.sql") as f:
        conn.executescript(f.read())
    cache = load_cache()
    try:
        movies, ratings = extract()
        movies, movie_genres, ratings = transform(movies, ratings, cache)
    finally:
        save_cache(cache)
    load(conn, movies, movie_genres, ratings)
    for t in ("movies", "genres", "movie_genres", "ratings"):
        log.info("%s: %d rows", t, conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0])
    conn.close()


if __name__ == "__main__":
    main()
