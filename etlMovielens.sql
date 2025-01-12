CREATE OR REPLACE STAGE my_stage;
CREATE database Platypus_Movielens;
Use database Platypus_Movielens;
create stage platypus;
create schema movielens_schema;
use schema movielens_schema;

CREATE TABLE age_group (
    id INT PRIMARY KEY,
    name VARCHAR(45)
);


CREATE OR REPLACE TABLE occupations (
    id INT PRIMARY KEY,
    name VARCHAR(255)
);

CREATE  OR REPLACE TABLE users (
    id INT PRIMARY KEY,
    age INT,
    gender CHAR(1),
    occupation_id INT,
    zip_code VARCHAR(255),
    FOREIGN KEY (occupation_id) REFERENCES occupations(id)
);

CREATE TABLE movies (
    id INT PRIMARY KEY,
    title VARCHAR(255),
    release_year CHAR(4)
);

CREATE TABLE genres (
    id INT PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE genres_movies (
    id INT PRIMARY KEY,
    movie_id INT,
    genre_id INT,
    FOREIGN KEY (movie_id) REFERENCES movies(id),
    FOREIGN KEY (genre_id) REFERENCES genres(id)
);

CREATE TABLE ratings (
    id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    rating INT,
    rated_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (movie_id) REFERENCES movies(id)
);

CREATE TABLE tags (
    id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    tags VARCHAR(4000),
    created_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (movie_id) REFERENCES movies(id)
);

CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  FIELD_DELIMITER = ','
  NULL_IF = ('NULL', 'null');

  
COPY INTO movies
FROM @my_stage/movies.csv
FILE_FORMAT = my_csv_format
ON_ERROR = 'CONTINUE';

COPY INTO occupations
FROM @my_stage/occupations.csv
FILE_FORMAT = my_csv_format
ON_ERROR = 'CONTINUE';

COPY INTO tags
FROM @my_stage/tags.csv
FILE_FORMAT = my_csv_format
ON_ERROR = 'CONTINUE';

COPY INTO genres
FROM @my_stage/genres.csv
FILE_FORMAT = my_csv_format
ON_ERROR = 'CONTINUE';

COPY INTO genres_movies
FROM @my_stage/genres_movies.csv
FILE_FORMAT = my_csv_format
ON_ERROR = 'CONTINUE';

COPY INTO age_group
FROM @my_stage/age_group.csv
FILE_FORMAT = my_csv_format
ON_ERROR = 'CONTINUE';

COPY INTO ratings
FROM @my_stage/ratings.csv
FILE_FORMAT = my_csv_format
ON_ERROR = 'CONTINUE';

COPY INTO users
FROM @my_stage/users1.csv
FILE_FORMAT = my_csv_format
ON_ERROR = 'CONTINUE';

select * from users;

CREATE OR REPLACE TABLE dim_users AS
SELECT 
    u.id AS dim_userId,
    u.gender,
    u.zip_code,
    CASE 
        WHEN u.age < 18 THEN 'Under 18'
        WHEN u.age BETWEEN 18 AND 24 THEN '18-24'
        WHEN u.age BETWEEN 25 AND 34 THEN '25-34'
        WHEN u.age BETWEEN 35 AND 44 THEN '35-44'
        WHEN u.age BETWEEN 45 AND 54 THEN '45-54'
        WHEN u.age >= 55 THEN '55+'
        ELSE 'Unknown'
    END AS age_group,
    o.name AS occupation
FROM 
    users u
LEFT JOIN 
    occupations o 
    ON u.occupation_id = o.id;
    
select * from dim_users;

    
CREATE OR REPLACE TABLE dim_movies AS
SELECT 
    m.id AS dim_movieId,
    m.title,
    m.release_year,
    LISTAGG(g.name, ', ') WITHIN GROUP (ORDER BY g.name) AS genres,
    LISTAGG(t.tags, ', ') WITHIN GROUP (ORDER BY t.tags) AS tags
FROM 
    movies m
LEFT JOIN 
    genres_movies gm ON m.id = gm.movie_id
LEFT JOIN 
    genres g ON gm.genre_id = g.id
LEFT JOIN 
    tags t ON m.id = t.movie_id
GROUP BY 
    m.id, m.title, m.release_year;

select * from dim_movies;



CREATE OR REPLACE TABLE fact_ratings AS
SELECT 
    r.id AS fact_ratingId,
    r.rating,
    r.rated_at,
    r.user_id AS userId,
    LISTAGG(t.id, ', ') WITHIN GROUP (ORDER BY t.id) AS tagIds,
    r.movie_id AS movieId,
    DATE_PART('epoch_second', r.rated_at) AS dateId
FROM 
    ratings r
LEFT JOIN 
    tags t ON r.user_id = t.user_id AND r.movie_id = t.movie_id
GROUP BY 
    r.id, r.rating, r.rated_at, r.user_id, r.movie_id;


    
select * from fact_ratings;




CREATE OR REPLACE TABLE dim_tags AS
SELECT 
    t.id AS dim_tagsId,
    t.created_at,
    t.tags,
    t.user_id,
    t.movie_id
FROM 
    tags t;


select * from dim_tags;




CREATE OR REPLACE TABLE dim_date AS
SELECT 
    DISTINCT
    r.rated_at::DATE AS date,
    EXTRACT(MONTH FROM r.rated_at) AS month,
    EXTRACT(YEAR FROM r.rated_at) AS year
FROM 
    ratings r;

select * from dim_date;