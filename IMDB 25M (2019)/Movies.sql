



CREATE TABLE movie_list (
	movie_ID int,
    title text,
    genres text
);

COPY public.movie_list 
FROM 'C:\Users\Nicholas\Desktop\ml-25m\ml-25m\movies.csv'
WITH (FORMAT CSV, HEADER);


CREATE TABLE links (
	movie_ID int, -- same as in movie_list
    imdbID int,
    tmdbID int
);

COPY public.links 
FROM 'C:\Users\Nicholas\Desktop\ml-25m\ml-25m\links.csv'
WITH (FORMAT CSV, HEADER);


CREATE TABLE genome_tags (
	tag_ID int,
    tag text
);

COPY public.genome_tags 
FROM 'C:\Users\Nicholas\Desktop\ml-25m\ml-25m\genome-tags.csv'
WITH (FORMAT CSV, HEADER);


CREATE TABLE genome_scores (
	movie_ID int,
    tag_ID smallint,
    relevance numeric
);

COPY public.genome_scores
FROM 'C:\Users\Nicholas\Desktop\ml-25m\ml-25m\genome-scores.csv'
WITH (FORMAT CSV, HEADER);


CREATE TABLE ratings (
	user_ID int,
    movie_ID int,
    rating numeric,
    submit_time_epoch varchar(10), --need to convert to timestamp
    submit_time timestamp
);


COPY public.ratings (user_ID, movie_ID, rating, submit_time_epoch)
FROM 'C:\Users\Nicholas\Desktop\ml-25m\ml-25m\ratings.csv'
WITH (FORMAT CSV, HEADER);

CREATE TABLE tags (
	user_ID int,
    movie_ID int,
    tag text,
    submit_time_epoch varchar(10), --need to convert to timestamp
    submit_time timestamp
);

COPY public.tags (user_ID, movie_ID, tag, submit_time_epoch)
FROM 'C:\Users\Nicholas\Desktop\ml-25m\ml-25m\tags.csv'
WITH (FORMAT CSV, HEADER);

--change seconds after 1-1-1970 to time in UTC
UPDATE ratings
SET submit_time = to_timestamp(submit_time_epoch::numeric)::timestamp

UPDATE tags
SET submit_time = to_timestamp(submit_time_epoch::numeric)::timestamp

--everything above this is to copy/set data into tables
----------------------------------------------------------------------------------



--combine both tables then figure out which tags appear the most in the movies
SELECT * 
FROM genome_scores JOIN genome_tags
ON genome_scores.tag_ID = genome_tags.tag_ID

--tasks done

--list of top 100 most rated movies
--includes title of the movies via JOIN
--might want to save it to a temp table to compare with another joined table
SELECT COUNT(r.movie_ID), ml.title, AVG(r.rating)
FROM public.ratings AS r JOIN public.movie_list AS ml
ON r.movie_ID = ml.movie_ID
GROUP BY ml.title
ORDER BY COUNT(r.movie_ID) DESC
LIMIT 100

--create a new table to calculate percentiles from
CREATE TABLE avg_movie_ratings AS
SELECT COUNT(r.movie_ID), ml.title, AVG(r.rating)
FROM public.ratings AS r JOIN public.movie_list AS ml
ON r.movie_ID = ml.movie_ID
GROUP BY ml.title
ORDER BY AVG(r.rating) DESC

--SUBQUERY
--top 10 percent of movies have at least 414 ratings
SELECT
	percentile_cont(.9)
	WITHIN GROUP (ORDER BY count) AS top10_percent
FROM public.avg_movie_ratings

--added above as subquery to filter out top 10% of movies rated
SELECT COUNT(r.movie_ID), ml.title, round(AVG(r.rating),2 )
FROM public.ratings AS r JOIN public.movie_list AS ml
ON r.movie_ID = ml.movie_ID
GROUP BY ml.title
HAVING COUNT(r.movie_ID) >= (SELECT
	percentile_cont(.9)
	WITHIN GROUP (ORDER BY count) AS top10_percent
	FROM public.avg_movie_ratings)
ORDER BY AVG(r.rating) DESC
LIMIT 100

--quartiles of top 10% by rating
SELECT unnest(
	percentile_cont(ARRAY[.25,.5,.75])
	WITHIN GROUP (ORDER BY rating)
) AS quartiles
FROM public.ratings AS r JOIN public.movie_list AS ml
ON r.movie_ID = ml.movie_ID
HAVING COUNT(r.movie_ID) >= (SELECT --filter out only top 10% of movies rated
	percentile_cont(.9)
	WITHIN GROUP (ORDER BY count ASC) AS top10_percent
	FROM public.avg_movie_ratings)
ORDER BY AVG(r.rating) DESC
--top 25% = 4, median = 3.5, top 75% = 3

--average rating of top 10% most rated movies = 3.53
SELECT round(avg(r.rating), 2) as avg_top10perc_rating
FROM public.ratings as r
HAVING COUNT(r.movie_ID) >= (SELECT --filter out only top 10% of movies rated
	percentile_cont(.9)
	WITHIN GROUP (ORDER BY count) AS top10_percent
	FROM public.avg_movie_ratings)


CREATE TABLE movie_genres(
	movie_ID int,
    title text,
    genre text
);

CREATE TABLE temp_movies(
	movie_ID int,
    title text,
    genres_og text
);

INSERT INTO temp_movies(movie_ID, title, genres_og)
SELECT movie_id, title, genres
FROM public.movie_list;

--parse each genre of that movie into a new row 
INSERT INTO movie_genres(movie_id, title, genre)
SELECT temp_movies.movie_id, temp_movies.title,
    CASE WHEN temp_movies.genres_og LIKE '%|%' 
        THEN substring(genres_og, 1, POSITION('|' in temp_movies.genres_og) - 1)
    ELSE temp_movies.genres_og
	END
FROM public.temp_movies;

UPDATE temp_movies
SET genres_og = CASE
	WHEN genres_og LIKE '%|%'
        THEN RIGHT(genres_og, LENGTH(genres_og) - POSITION('|' in genres_og))
    ELSE NULL
    END;

DELETE FROM temp_movies
WHERE temp_movies.genres_og IS NULL;

SELECT COUNT(*)
FROM public.temp_movies;

--repeat above steps from UPDATE query until no more rows are in the temp_movies table

DELETE TABLE temp_movies

DELETE FROM public.movie_genres
WHERE movie_genres.genre LIKE '(no genres listed)';

--personal note
--we have the genres parsed out into separate rows, now we can count them and even look at years that the movies came out. maybe add year column


ALTER TABLE movie_genres
ADD movie_year text;

UPDATE public.movie_genres
SET movie_year = regexp_match(title, '\(\d{4}\)')

UPDATE public.movie_genres
SET movie_year = replace(movie_year, '{', '')

UPDATE public.movie_genres
SET movie_year = replace(movie_year, '}', '')

UPDATE public.movie_genres
SET movie_year = replace(movie_year, '(', '')

UPDATE public.movie_genres
SET movie_year = replace(movie_year, ')', '')

--check to see how many movies do not have the year
SELECT *
FROM public.movie_genres
WHERE movie_year IS NULL 

--227 movies deleted from the table
DELETE FROM movie_genres
WHERE movie_year IS NULL

--


--genome scores are useless, so extract the genres from movies.csv
--first need to join movies.csv with ratings.csv
--set list of genres are in README. Write a function which checks if it matches one of the genres.
--extract that into a new row and delete that genre from the previous set of genres.



--figure out median of total ratings for movies.
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary DESC) 


--tasks to do
--1. use rating.csv to figure out most rated/reviewed movies
    --join ratings and movies to figure this out
    --DONE

--2. sort out the most occurring tags from the top 5% or top 10% most rated movies
    --figure this out by filtering via percentile and table join above

--first make CTE of top 10% of most rated movies
--from that CTE also filter out top 25%, 50%, 75% of higest rated movies

--join movie_genres table with CTE to find out genres that occurred most frequently in top _ percent of highest rated movies
--this has the information for the top genres of the top 10% most rated movies
with t10PercentMovies AS (
    SELECT COUNT(r.movie_ID), ml.title, round(AVG(r.rating),2 )
    FROM public.ratings AS r JOIN public.movie_list AS ml
    ON r.movie_ID = ml.movie_ID
    GROUP BY ml.title
    HAVING COUNT(r.movie_ID) >= (SELECT
        percentile_cont(.9)
        WITHIN GROUP (ORDER BY count) AS top10_percent
        FROM public.avg_movie_ratings)
    ORDER BY AVG(r.rating) DESC
)

SELECT movie_genres.genre, COUNT(movie_genres.genre)
FROM t10PercentMovies JOIN public.movie_genres
ON t10PercentMovies.title = movie_genres.title
GROUP BY movie_genres.genre
ORDER BY count(movie_genres.genre) DESC

--we can modify the query above to filter out top 25% highest rated movies out of the top 10% most rated movies
--idea - figure out the difference in percent of movies in each genre and compare to each quartile
with t10PercentMovies AS (
    SELECT COUNT(r.movie_ID), ml.title, round(AVG(r.rating),2 )
    FROM public.ratings AS r JOIN public.movie_list AS ml
    ON r.movie_ID = ml.movie_ID
    GROUP BY ml.title
    HAVING COUNT(r.movie_ID) >= (SELECT
        percentile_cont(.9)
        WITHIN GROUP (ORDER BY count) AS top10_percent
        FROM public.avg_movie_ratings)
		AND
		round(AVG(r.rating),2 ) >= 3.5 --modify this line to filter out top x percent of higest rated movies
    ORDER BY AVG(r.rating) DESC
)

SELECT movie_genres.genre, COUNT(movie_genres.genre)
FROM t10PercentMovies JOIN public.movie_genres
ON t10PercentMovies.title = movie_genres.title
GROUP BY movie_genres.genre
ORDER BY count(movie_genres.genre) DESC









--EXPORT DATA BELOW

--Top 25 quartile
COPY(
    with t10PercentMovies AS (
        SELECT COUNT(r.movie_ID), ml.title, round(AVG(r.rating),2 )
        FROM public.ratings AS r JOIN public.movie_list AS ml
        ON r.movie_ID = ml.movie_ID
        GROUP BY ml.title
        HAVING COUNT(r.movie_ID) >= (SELECT
            percentile_cont(.9)
            WITHIN GROUP (ORDER BY count) AS top10_percent
            FROM public.avg_movie_ratings)
            AND
            round(AVG(r.rating),2 ) >= 4 --modify this line to filter out top x percent of higest rated movies
        ORDER BY AVG(r.rating) DESC
    )
    SELECT movie_genres.genre, COUNT(movie_genres.genre)
    FROM t10PercentMovies JOIN public.movie_genres
    ON t10PercentMovies.title = movie_genres.title
    GROUP BY movie_genres.genre
    ORDER BY count(movie_genres.genre) DESC
)
TO 'C:\Users\Nicholas\Desktop\ml-25m\Export Data\top10PercTQ25.csv'
WITH (FORMAT CSV, HEADER)

--top 50 quartile
COPY(
    with t10PercentMovies AS (
        SELECT COUNT(r.movie_ID), ml.title, round(AVG(r.rating),2 )
        FROM public.ratings AS r JOIN public.movie_list AS ml
        ON r.movie_ID = ml.movie_ID
        GROUP BY ml.title
        HAVING COUNT(r.movie_ID) >= (SELECT
            percentile_cont(.9)
            WITHIN GROUP (ORDER BY count) AS top10_percent
            FROM public.avg_movie_ratings)
            AND
            round(AVG(r.rating),2 ) >= 3.5 --modify this line to filter out top x percent of higest rated movies
        ORDER BY AVG(r.rating) DESC
    )
    SELECT movie_genres.genre, COUNT(movie_genres.genre)
    FROM t10PercentMovies JOIN public.movie_genres
    ON t10PercentMovies.title = movie_genres.title
    GROUP BY movie_genres.genre
    ORDER BY count(movie_genres.genre) DESC
)
TO 'C:\Users\Nicholas\Desktop\ml-25m\Export Data\top10PercTQ50.csv'
WITH (FORMAT CSV, HEADER)

--top 75 quartile
COPY(
    with t10PercentMovies AS (
        SELECT COUNT(r.movie_ID), ml.title, round(AVG(r.rating),2 )
        FROM public.ratings AS r JOIN public.movie_list AS ml
        ON r.movie_ID = ml.movie_ID
        GROUP BY ml.title
        HAVING COUNT(r.movie_ID) >= (SELECT
            percentile_cont(.9)
            WITHIN GROUP (ORDER BY count) AS top10_percent
            FROM public.avg_movie_ratings)
            AND
            round(AVG(r.rating),2 ) >= 3 --modify this line to filter out top x percent of higest rated movies
        ORDER BY AVG(r.rating) DESC
    )
    SELECT movie_genres.genre, COUNT(movie_genres.genre)
    FROM t10PercentMovies JOIN public.movie_genres
    ON t10PercentMovies.title = movie_genres.title
    GROUP BY movie_genres.genre
    ORDER BY count(movie_genres.genre) DESC
)
TO 'C:\Users\Nicholas\Desktop\ml-25m\Export Data\top10PercTQ75.csv'
WITH (FORMAT CSV, HEADER)


--top 10% most rated movies. Use this to find bottom x %
COPY(
    with t10PercentMovies AS (
        SELECT COUNT(r.movie_ID), ml.title, round(AVG(r.rating),2 )
        FROM public.ratings AS r JOIN public.movie_list AS ml
        ON r.movie_ID = ml.movie_ID
        GROUP BY ml.title
        HAVING COUNT(r.movie_ID) >= (SELECT
            percentile_cont(.9)
            WITHIN GROUP (ORDER BY count) AS top10_percent
            FROM public.avg_movie_ratings)
        ORDER BY AVG(r.rating) DESC
    )
    SELECT movie_genres.genre, COUNT(movie_genres.genre)
    FROM t10PercentMovies JOIN public.movie_genres
    ON t10PercentMovies.title = movie_genres.title
    GROUP BY movie_genres.genre
    ORDER BY count(movie_genres.genre) DESC
)
TO 'C:\Users\Nicholas\Desktop\ml-25m\Export Data\top10PercentRated.csv'
WITH (FORMAT CSV, HEADER)

--sort by year and what is the top 3 movies and movie genres per year (2 separate queries)

--top 10% of movies grouped by year and movie genre (put in a table to graph)
--use this to find trend of popular genres per year as time goes on
COPY(
    with t10PercentMovies AS (
        SELECT COUNT(r.movie_ID), ml.title, round(AVG(r.rating),2 )
        FROM public.ratings AS r JOIN public.movie_list AS ml
        ON r.movie_ID = ml.movie_ID
        GROUP BY ml.title
        HAVING COUNT(r.movie_ID) >= (SELECT
            percentile_cont(.9)
            WITHIN GROUP (ORDER BY count) AS top10_percent
            FROM public.avg_movie_ratings)
        ORDER BY AVG(r.rating) DESC
    )

    SELECT movie_genres.movie_year, movie_genres.genre, COUNT(movie_genres.genre), 
        RANK() OVER (PARTITION BY movie_genres.movie_year ORDER BY COUNT(movie_genres.genre)) AS movie_rank
    FROM t10PercentMovies JOIN public.movie_genres
    ON t10PercentMovies.title = movie_genres.title
    GROUP BY movie_genres.movie_year, movie_genres.genre
    ORDER BY movie_genres.movie_year DESC, movie_genres.genre ASC
)
TO 'C:\Users\Nicholas\Desktop\ml-25m\Export Data\top10ByYear.csv'
WITH (FORMAT CSV, HEADER)

--top 3 genres per year. Might have more than 3 genres due to a tie in prevalence of genres for that year
COPY(
    with top3GenresByYear AS (
        with t10PercentMovies AS (
            SELECT COUNT(r.movie_ID), ml.title, round(AVG(r.rating),2 )
            FROM public.ratings AS r JOIN public.movie_list AS ml
            ON r.movie_ID = ml.movie_ID
            GROUP BY ml.title
            HAVING COUNT(r.movie_ID) >= (SELECT
                percentile_cont(.9)
                WITHIN GROUP (ORDER BY count) AS top10_percent
                FROM public.avg_movie_ratings)
            ORDER BY AVG(r.rating) DESC
        )

        SELECT movie_genres.movie_year, movie_genres.genre, COUNT(movie_genres.genre), 
            RANK() OVER (PARTITION BY movie_genres.movie_year ORDER BY COUNT(movie_genres.genre) DESC) AS movie_rank
        FROM t10PercentMovies JOIN public.movie_genres
        ON t10PercentMovies.title = movie_genres.title
        GROUP BY movie_genres.movie_year, movie_genres.genre
        ORDER BY movie_genres.movie_year DESC, COUNT(movie_genres.genre) ASC
    )
    SELECT *
    FROM top3GenresByYear
    WHERE movie_rank < 4
    GROUP BY top3GenresByYear.movie_year, top3GenresByYear.genre, top3genresbyyear.count, top3GenresByYear.movie_rank
    ORDER BY top3GenresByYear.movie_year DESC, COUNT(top3GenresByYear.genre) ASC
)
TO 'C:\Users\Nicholas\Desktop\ml-25m\Export Data\top3GenresByYear.csv'
WITH (FORMAT CSV, HEADER)


--top 3 movies per year by rating (by popularity would be how many times the movie was rated)
COPY(
    with t3MoviesPerYear AS (
        with t10PercentMovies AS (
            SELECT COUNT(r.movie_ID), ml.title, round(AVG(r.rating),2 )
            FROM public.ratings AS r JOIN public.movie_list AS ml
            ON r.movie_ID = ml.movie_ID
            GROUP BY ml.title
            HAVING COUNT(r.movie_ID) >= (SELECT
                percentile_cont(.9)
                WITHIN GROUP (ORDER BY count) AS top10_percent
                FROM public.avg_movie_ratings)
            ORDER BY AVG(r.rating) DESC
        )
        SELECT mg.movie_year, t10.title, t10.round,
            RANK() OVER (PARTITION BY mg.movie_year ORDER BY t10.round DESC) AS movie_rank
        FROM t10PercentMovies AS t10 JOIN public.movie_genres AS mg
        ON t10.title = mg.title
        GROUP BY mg.movie_year, t10.title, t10.round
        ORDER BY mg.movie_year ASC
    )
    SELECT *
    FROM t3MoviesPerYear
    WHERE movie_rank < 4
)
TO 'C:\Users\Nicholas\Desktop\ml-25m\Export Data\top3RatedMoviesByYear.csv'
WITH (FORMAT CSV, HEADER)


--top 3 movies per year by popularity (times the movie was rated)
COPY(
    with t3MoviesPerYear AS (
        with t10PercentMovies AS (
            SELECT COUNT(r.movie_ID), ml.title, round(AVG(r.rating),2 )
            FROM public.ratings AS r JOIN public.movie_list AS ml
            ON r.movie_ID = ml.movie_ID
            GROUP BY ml.title
            HAVING COUNT(r.movie_ID) >= (SELECT
                percentile_cont(.9)
                WITHIN GROUP (ORDER BY count) AS top10_percent
                FROM public.avg_movie_ratings)
            ORDER BY AVG(r.rating) DESC
        )
        SELECT mg.movie_year, t10.title, t10.count,
            RANK() OVER (PARTITION BY mg.movie_year ORDER BY t10.count DESC) AS movie_rank
        FROM t10PercentMovies AS t10 JOIN public.movie_genres AS mg
        ON t10.title = mg.title
        GROUP BY mg.movie_year, t10.title, t10.count
        ORDER BY mg.movie_year ASC
    )
    SELECT *
    FROM t3MoviesPerYear
    WHERE movie_rank < 4
)
TO 'C:\Users\Nicholas\Desktop\ml-25m\Export Data\top3PopularMoviesByYear.csv'
WITH (FORMAT CSV, HEADER)