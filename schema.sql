CREATE TABLE IF NOT EXISTS movies (
    movie_id     INTEGER PRIMARY KEY,
    title        TEXT NOT NULL,
    release_year INTEGER,
    decade       INTEGER,
    imdb_id      TEXT,
    director     TEXT,
    plot         TEXT,
    box_office   INTEGER,
    omdb_found   INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS genres (
    genre_id INTEGER PRIMARY KEY,
    name     TEXT UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id INTEGER,
    genre_id INTEGER,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
);
CREATE TABLE IF NOT EXISTS ratings (
    user_id  INTEGER,
    movie_id INTEGER,
    rating   REAL NOT NULL,
    rated_at TEXT,
    PRIMARY KEY (user_id, movie_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);