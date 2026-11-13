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
available movies. Optional setting: `OMDB_MAX_CALLS` in `.env` (default 950) changes how many new API calls one run may make.

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

## Results

Run on MovieLens `ml-latest-small`: **9,742 movies, 100,836 ratings, 19 genres**.
Running `python etl.py` repeatedly gives identical row counts (9,742 / 19 / 22,050 / 100,836),
so the pipeline is idempotent.

| # | Question | Answer |
|---|----------|--------|
| 1 | Movie with the highest average rating (min. 50 ratings) | **The Shawshank Redemption**, 4.43 (317 ratings) |
| 2 | Top 5 genres by average rating | Film-Noir 3.92, War 3.81, Documentary 3.80, Drama 3.66, Crime 3.66 |
| 3 | Director with the most movies | **Alfred Hitchcock**, 10 movies (see note below) |
| 4 | Average rating per release year | 106 years (1902-2018), see highlights below |

**Q4 highlights:** 96 of the 106 years have an average between 3.3 and 4.0, and 2010-2018 sit between
3.39 and 3.58. The extreme values come from tiny samples: 1917 (4.5), 1915 and 1919 (2.0) each rest on a
single rating, and 1902-1908 on 1-5 ratings. Among the 41 years with at least 500 ratings, averages
range only from 3.34 to 3.88, so those are the more reliable numbers.

**Notes**
- Q1, Q2 and Q4 use all 9,742 movies. **Q3 depends on OMDb data**, which is only available for the
  movies fetched so far (602 of 9,742). The free OMDb key allows about 1,000 calls per day, so the answer
  may change as daily reruns fetch more movies.
- Q2 averages every rating of every movie in a genre, so a movie with several genres counts once in each.
- Movies with fewer than 50 ratings are excluded from Q1 on purpose.

## Design choices and assumptions
- **pandas** cleans and reshapes the data (titles, genres, timestamps, missing values), and
  **sqlite3** loads it. `requests` talks to the OMDb API.
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