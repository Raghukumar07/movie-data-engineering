-- 1. Highest average rating. Movies need >= 50 ratings, otherwise a single 5-star vote wins
SELECT m.title, ROUND(AVG(r.rating),2) AS avg_rating, COUNT(*) AS n
FROM ratings r JOIN movies m USING(movie_id)
GROUP BY m.movie_id HAVING n >= 50
ORDER BY avg_rating DESC LIMIT 1;

-- 2. Top 5 genres by average rating
SELECT g.name, ROUND(AVG(r.rating),2) AS avg_rating
FROM ratings r
JOIN movie_genres mg USING(movie_id)
JOIN genres g USING(genre_id)
GROUP BY g.genre_id ORDER BY avg_rating DESC LIMIT 5;

-- 3. Director with the most movies
SELECT director, COUNT(*) AS movie_count
FROM movies WHERE director IS NOT NULL
GROUP BY director ORDER BY movie_count DESC LIMIT 1;

-- 4. Average rating by release year
SELECT m.release_year, ROUND(AVG(r.rating),2) AS avg_rating
FROM ratings r JOIN movies m USING(movie_id)
WHERE m.release_year IS NOT NULL
GROUP BY m.release_year ORDER BY m.release_year;