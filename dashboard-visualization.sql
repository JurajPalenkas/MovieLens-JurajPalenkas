-- Filmy s najväčším počtom hodnotení.--
SELECT 
    dm.title AS movie_title,
    COUNT(fr.fact_ratingId) AS total_ratings
FROM 
    fact_ratings fr
JOIN 
    dim_movies dm ON fr.movieId = dm.dim_movieId
GROUP BY 
    dm.title
ORDER BY 
    total_ratings DESC
LIMIT 10;

--Používateľia podľa vekových skupín -- 
SELECT 
    age_group,
    COUNT(*) AS user_count
FROM 
    dim_users
GROUP BY 
    age_group;

-- Žánre s najväčším počtom filmov -- 
SELECT 
    g.name AS genre,
    COUNT(m.id) AS movie_count
FROM 
    genres g
JOIN 
    genres_movies gm ON g.id = gm.genre_id
JOIN 
    movies m ON gm.movie_id = m.id
GROUP BY 
    g.name
ORDER BY 
    movie_count DESC;

--Počet filmov danom roku --
SELECT 
    m.release_year,
    COUNT(*) AS movie_count
FROM 
    movies m
GROUP BY 
    m.release_year
ORDER BY 
    release_year DESC;

--Najviac používané tagy -- 
SELECT 
    t.tags,
    COUNT(*) AS tag_count
FROM 
    tags t
GROUP BY 
    t.tags
ORDER BY 
    tag_count DESC;