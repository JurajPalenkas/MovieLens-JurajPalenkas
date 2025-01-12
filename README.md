  

# **ETL proces datasetu Platypus_Movielens**

  

Tento dokument popisuje implementáciu ETL procesu v Snowflake pre analýzu dát z **Platypus_Movielens** datasetu. Projekt sa zameriava na analýzu používateľských hodnotení filmov a ich preferencií, vrátane analýzy demografických údajov používateľov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrik.

  

---

## **1. Úvod a popis zdrojových dát**

Cieľom tohto projektu je analyzovať dáta o filmoch, používateľoch a ich hodnoteniach, aby bolo možné identifikovať trendy, najpopulárnejšie filmy a správanie používateľov.

  

Zdrojové dáta pochádzajú zo systému, ktorý poskytuje údaje v CSV formáte, rozdelené do niekoľkých tabuliek:

-  `movies`

-  `ratings`

-  `users`

-  `occupations`

-  `genres`

-  `genres_movies`

-  `tags`

  

Účelom ETL procesu je tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

  

---

### **1.1 Dátová architektúra**

  

### **ERD diagram**

Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**. Diagram zobrazuje prepojenia medzi tabuľkami, ako sú cudzie kľúče a vzťahy.
<p align="center">
  <img src="https://github.com/JurajPalenkas/MovieLens-JurajPalenkas/blob/main/MovieLens_ERD%20(1).png?raw=true" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma MovieLens</em>
</p>
  

---

## **2 Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, pre efektívnu analýzu, kde centrálny bod predstavuje faktová tabuľka **`fact_ratings`**, ktorá je prepojená s nasledujúcimi dimenziami:

-  **`dim_movies`**: Obsahuje podrobné informácie o filmoch (názov, rok vydania, žánre, tagy).

-  **`dim_users`**: Obsahuje demografické údaje o používateľoch, ako sú vekové kategórie, pohlavie a povolanie.

-  **`dim_date`**: Obsahuje informácie o dátumoch hodnotení (deň, mesiac, rok).

-  **`dim_tags`**: Obsahuje informácie o tagoch priradených k filmom používateľmi.

  <p align="center">
  <img src="https://github.com/JurajPalenkas/MovieLens-JurajPalenkas/blob/main/movielensJP.png?raw=true" alt="ERD Schema">
  <br>
  <em>Obrázok 2  Schéma hviezdy MovieLens</em>
</p>

---

## **3. ETL proces v Snowflake**

ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces bol implementovaný v Snowflake a zahŕňa nasledujúce kroky:

  

---

### **3.1 Extract (Extrahovanie dát)**

Dáta zo zdrojových CSV súborov boli nahraté do Snowflake prostredníctvom interného stage úložiska `my_stage`. Vytvorenie stage bolo zabezpečené príkazom:

  

```sql

CREATE OR REPLACE STAGE my_stage;

```

  

Každý CSV súbor bol nahratý do stage a importovaný do staging tabuliek pomocou príkazu `COPY INTO`. Príklad:

  

```sql

COPY INTO movies

FROM @my_stage/movies.csv

FILE_FORMAT = my_csv_format

ON_ERROR =  'CONTINUE';

```

  

---

### **3.2 Transform (Transformácia dát)**

V tejto fáze boli dáta transformované do viacdimenzionálneho modelu.

  

#### Dimenzia `dim_users`

Transformácia zahŕňala pridelenie vekových kategórií používateľom a obohatenie údajov o povolania:

  

```sql

CREATE OR REPLACE TABLE dim_users AS

SELECT

u.id AS dim_userId,

u.gender,

u.zip_code,

CASE

WHEN u.age <  18 THEN 'Under 18'

WHEN u.age BETWEEN 18 AND 24 THEN '18-24'

WHEN u.age BETWEEN 25 AND 34 THEN '25-34'

WHEN u.age BETWEEN 35 AND 44 THEN '35-44'

WHEN u.age BETWEEN 45 AND 54 THEN '45-54'

WHEN u.age >=  55 THEN '55+'

ELSE 'Unknown'

END AS age_group,

o.name AS occupation

FROM

users u

LEFT JOIN

occupations o

ON u.occupation_id = o.id;

```

  

#### Dimenzia `dim_movies`

Získanie informácií o filmoch, vrátane zoznamu žánrov a tagov:

  

```sql

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

```

  

#### Faktová tabuľka `fact_ratings`

Vytvorenie tabuľky obsahujúcej hodnotenia používateľov:

  

```sql

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

```

  

---

### **3.3 Load (Načítanie dát)**

Po úspešnej transformácii boli dáta nahraté do finálnych tabuliek. Na záver boli staging tabuľky odstránené:

  

```sql

DROP TABLE IF EXISTS movies_staging;

DROP TABLE IF EXISTS users_staging;

DROP TABLE IF EXISTS ratings_staging;

DROP TABLE IF EXISTS occupations_staging;

DROP TABLE IF EXISTS genres_staging;

DROP TABLE IF EXISTS tags_staging;

```

  

---

## **4 Vizualizácia dát**

  

Vizualizácie môžu zahŕňať napríklad:
<p align="center">
  <img src="https://github.com/JurajPalenkas/MovieLens-JurajPalenkas/blob/main/dashboard.jpg?raw=true" alt="ERD Schema">
  <br>
  <em>Obrázok 3 Dashboard MovieLens datasetu</em>
</p>
  

### **Graf 1: Filmy s najväčším počtom hodnotení**

```sql

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

```

  

### **Graf 2: Používateľia podľa vekových skupín**

```sql

SELECT

age_group,

COUNT(*) AS user_count

FROM

dim_users

GROUP BY

age_group;

```

  

### **Graf 3: Žánre s najväčším počtom filmov**

```sql

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

```

  

### **Graf 4: Počet filmov v danom roku**

```sql

SELECT

m.release_year,

COUNT(*) AS movie_count

FROM

movies m

GROUP BY

m.release_year

ORDER BY

release_year DESC;

```

  

### **Graf 5: Najviac používané tagy**

```sql

SELECT

t.tags,

COUNT(*) AS tag_count

FROM

tags t

GROUP BY

t.tags

ORDER BY

tag_count DESC;

```

  

---

**Autor:** [Juraj Pálenkáš]