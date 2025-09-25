USE BASES2_PROYECTOS;

DROP PROCEDURE IF EXISTS show_info_movie_show;
-- PRIMER PROCEDIMIENTO 
CREATE PROCEDURE show_info_movie_show(IN p_title VARCHAR(1024))
BEGIN
  WITH candidates AS (
    SELECT tb.tconst
    FROM title_basics tb 
    WHERE tb.primaryTitle = p_title

    UNION
    SELECT tb.tconst
    FROM title_basics tb
    WHERE tb.originalTitle = p_title

    UNION
    SELECT a.titleId AS tconst
    FROM akas a          
    WHERE a.title = p_title
  ),
  uniq AS (
    SELECT DISTINCT tconst FROM candidates
  )
  SELECT
    tb.tconst,
    tb.titleType,
    tb.primaryTitle,
    tb.originalTitle,
    tb.isAdult,
    tb.startYear,
    tb.endYear,
    tb.runtimeMinutes,
    GROUP_CONCAT(DISTINCT bg.genre ORDER BY bg.genre SEPARATOR ', ') AS genres,
    r.averageRating,
    r.numVotes,
  
    MAX(epar.parentTconst IS NOT NULL) AS isSeries,
    MAX(eepi.tconst      IS NOT NULL) AS isEpisode
  FROM uniq u
  JOIN title_basics tb  ON tb.tconst = u.tconst
  LEFT JOIN ratings r   ON r.tconst  = tb.tconst
  LEFT JOIN basics_genres bg ON bg.tconst = tb.tconst
  LEFT JOIN episodes epar ON epar.parentTconst = tb.tconst
  LEFT JOIN episodes eepi ON eepi.tconst = tb.tconst
  GROUP BY
    tb.tconst, tb.titleType, tb.primaryTitle, tb.originalTitle,
    tb.isAdult, tb.startYear, tb.endYear, tb.runtimeMinutes, r.averageRating, r.numVotes
  ORDER BY tb.startYear, tb.primaryTitle;
END;

-- LAS REQUERIDAS EN EL CORREO
-- 1. Peliculas dirigidas por un director
-- =========================================================
DROP PROCEDURE IF EXISTS show_director_movies;
CREATE PROCEDURE show_director_movies(IN p_director VARCHAR(512))
BEGIN
  /* Películas (no episodios) dirigidas por p_director */
  SELECT
    nb.primaryName                              AS director,
    tb.tconst,
    tb.titleType,                               -- p.ej. movie, short, tvMovie
    tb.primaryTitle                             AS title,
    tb.originalTitle,
    tb.startYear,
    tb.runtimeMinutes,
    (SELECT GROUP_CONCAT(DISTINCT bg.genre ORDER BY bg.genre SEPARATOR ', ')
      FROM basics_genres bg
      WHERE bg.tconst = tb.tconst)             AS genres,
    r.averageRating,
    r.numVotes
  FROM name_basics     nb
  JOIN crew_directors  cd ON cd.nconst = nb.nconst
  JOIN title_basics    tb ON tb.tconst = cd.tconst
  LEFT JOIN ratings    r  ON r.tconst  = tb.tconst
  WHERE nb.primaryName = p_director
    AND NOT EXISTS (SELECT 1 FROM episodes e WHERE e.tconst = tb.tconst)  -- excluye episodios
  ORDER BY tb.startYear, tb.primaryTitle;
END;


-- 2. Top 10 películas con mejor rating
-- =========================================================
DROP PROCEDURE IF EXISTS show_top10_bestRatingMovies;
CREATE PROCEDURE show_top10_bestRatingMovies()
BEGIN
  SELECT
      tb.primaryTitle AS pelicula,
      GROUP_CONCAT(DISTINCT nb.primaryName ORDER BY nb.primaryName SEPARATOR ', ') AS directores,
      r.averageRating AS rating,
      tb.startYear     AS anio
  FROM title_basics tb
  JOIN ratings r           ON r.tconst = tb.tconst
  LEFT JOIN crew_directors cd ON cd.tconst = tb.tconst
  LEFT JOIN name_basics nb    ON nb.nconst = cd.nconst
  WHERE tb.titleType = 'movie'
  GROUP BY tb.tconst
  ORDER BY r.averageRating DESC, r.numVotes DESC, tb.primaryTitle ASC
  LIMIT 10;
END;

-- Prueba:
-- CALL show_top10_bestRatingMovies();


-- 3. Director con más películas
-- =========================================================
DROP PROCEDURE IF EXISTS show_director_most_movies;
CREATE PROCEDURE show_director_most_movies()
BEGIN
  SELECT
      nb.primaryName AS director,
      COUNT(DISTINCT tb.tconst) AS num_peliculas
  FROM name_basics nb
  JOIN crew_directors cd ON cd.nconst = nb.nconst
  JOIN title_basics tb   ON tb.tconst = cd.tconst
  WHERE tb.titleType = 'movie'
  GROUP BY nb.nconst
  ORDER BY num_peliculas DESC, nb.primaryName ASC
  LIMIT 1;
END;

-- Prueba:
-- CALL show_director_most_movies();


-- 4. Top 10 actores/actrices con mas peliculas
-- =========================================================
DROP PROCEDURE IF EXISTS show_top10_actor_most_movies;
CREATE PROCEDURE show_top10_actor_most_movies()
BEGIN
  SELECT
      nb.primaryName AS actor,
      COUNT(DISTINCT p.tconst) AS num_peliculas
  FROM principals p
  JOIN name_basics nb ON nb.nconst = p.nconst
  JOIN title_basics tb ON tb.tconst = p.tconst
  WHERE tb.titleType = 'movie'
    AND p.category IN ('actor','actress')
  GROUP BY nb.nconst
  ORDER BY num_peliculas DESC, nb.primaryName ASC
  LIMIT 10;
END;

-- Prueba:
-- CALL show_top10_actor_most_movies();

-- 5. Consulta en la calificacion
