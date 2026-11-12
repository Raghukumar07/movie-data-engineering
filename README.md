# Movie Data Pipeline

An ETL pipeline that combines the MovieLens "small" dataset (`movies.csv`, `ratings.csv`)
with movie details (director, plot, box office) from the OMDb API, and loads everything
into a SQLite database for analysis.

## Project structure
```
etl.py            # extract -> transform -> load
schema.sql        # CREATE TABLE statements
queries.sql       # the 4 analytical queries
requirements.txt
```

## How to run
1. Download `ml-latest-small.zip` from https://grouplens.org/datasets/movielens/latest/
   and put `movies.csv` and `ratings.csv` inside a `data/` folder.
2. Get a free API key from https://www.omdbapi.com/ and activate it via the email link.
   Put it in a `.env` file next to `etl.py` (this file is git-ignored):
   ```
   OMDB_API_KEY=your_key
   ```
3. Install and run:
   ```
   python -m venv .venv
   source .venv/bin/activate       # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   python etl.py
   ```
4. Run the queries: `sqlite3 movies.db < queries.sql` (or open `movies.db` in any SQLite viewer).

The free OMDb key allows about 1,000 calls per day, and there are about 9,700 movies.
Just rerun `python etl.py` each day until the log says API data was found for all
available movies. Optional settings: `OMDB_MAX_CALLS` (default 950), `DB_FILE`, `DATA_DIR`.

## How it works
- **Extract:** read both CSVs, then call OMDb by title and year for each movie.
- **Transform:**
  - Split `Matrix, The (1999)` into title `The Matrix` and year `1999`.
  - Compute `decade` (feature engineering).
  - Convert Unix timestamps to datetimes.
  - Split the `|`-separated genres into rows.
  - Turn OMDb's `N/A` into NULL and `$123,456` into an integer.
  - Drop duplicate ratings and ratings for unknown movies.
- **Load:** upsert into SQLite inside one transaction.

## Design choices and assumptions
- **SQLite:** zero setup, ships with Python. The SQL is standard enough to move to PostgreSQL.
- **Schema:** `movies`, `genres`, `movie_genres` (junction table for the many-to-many
  relationship) and `ratings`. Primary keys on `(user_id, movie_id)` and `(movie_id, genre_id)`
  prevent duplicates.
- **Idempotency:** movies are upserted (`ON CONFLICT DO UPDATE`), the rest use `INSERT OR IGNORE`.
  Running twice gives identical row counts.
- **API cache:** every response is saved in `omdb_cache.json`, so reruns make no repeated calls.
  Transient network errors are not cached and are retried next run.
- **Quota handling:** movies with the most ratings are fetched first, so the movies that matter
  most to the analysis get API data first. When the daily cap is hit, the script still loads
  everything else, and later runs upgrade rows via the upsert.
- **Movies not found in OMDb** are kept, with `omdb_found = 0` and NULL API fields.
- **Query 1** requires at least 50 ratings, otherwise a movie with a single 5-star vote would win.
- **Directors:** stored as OMDb returns them. Co-directed films ("A, B") count as one value in
  query 3. A separate `directors` table would fix this.

## Challenges and solutions
- **Title mismatches** (`Matrix, The (1999)` vs `The Matrix`): regex cleanup plus passing the
  release year to the API. Titles that still fail are kept without API data.
- **Daily API limit:** caching, a per-run call cap, and popularity-first ordering.
- **Messy API values** (`N/A`, currency strings): cleaning helper functions.
- **Idempotency:** primary keys plus upserts, verified by running the pipeline repeatedly.

## Possible improvements
- Use `links.csv` (IMDb IDs) for exact matching instead of titles.
- Separate `directors` table (many-to-many).
- PostgreSQL, scheduling with Airflow, parallel API calls with rate limiting,
  unit tests, and data-quality checks.
