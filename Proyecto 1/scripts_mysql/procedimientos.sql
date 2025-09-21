USE BASES2_PROYECTOS;

DROP PROCEDURE IF EXISTS show_info_movie_show;
-- PROCEDIMIENTO 
CREATE PROCEDURE show_info_movie_show(IN p_title VARCHAR(1024))
BEGIN
  /* ===================== 1) Ficha básica ===================== */
  SELECT 
    tb.tconst,
    tb.titleType,
    tb.primaryTitle,
    tb.originalTitle,
    tb.isAdult,
    tb.startYear,
    tb.endYear,
    tb.runtimeMinutes,
    (SELECT GROUP_CONCAT(DISTINCT bg.genre ORDER BY bg.genre SEPARATOR ', ')
      FROM basics_genres bg WHERE bg.tconst = tb.tconst) AS genres,
    r.averageRating,
    r.numVotes,
    EXISTS (SELECT 1 FROM episodes e WHERE e.parentTconst = tb.tconst LIMIT 1) AS isSeries,
    EXISTS (SELECT 1 FROM episodes e WHERE e.tconst       = tb.tconst LIMIT 1) AS isEpisode
  FROM title_basics tb
  LEFT JOIN ratings r ON r.tconst = tb.tconst
  WHERE tb.primaryTitle  = p_title
     OR tb.originalTitle = p_title
     OR EXISTS (SELECT 1 FROM akas a 
                WHERE a.titleId = tb.tconst AND a.title = p_title)
  ORDER BY tb.startYear, tb.primaryTitle;

  /* ===================== 2) AKAs + types/attributes ===================== */
  SELECT
    a.titleId AS tconst,
    a.ordering,
    a.title,
    a.region,
    a.isOriginalTitle,
    (SELECT GROUP_CONCAT(DISTINCT atp.type ORDER BY atp.type SEPARATOR ', ')
       FROM aka_types atp 
       WHERE atp.titleId = a.titleId AND atp.ordering = a.ordering) AS types,
    (SELECT GROUP_CONCAT(DISTINCT att.attribute ORDER BY att.attribute SEPARATOR ', ')
       FROM aka_attributes att
       WHERE att.titleId = a.titleId AND att.ordering = a.ordering) AS attributes
  FROM akas a
  WHERE EXISTS (
    SELECT 1 FROM title_basics tb
    WHERE tb.tconst = a.titleId
      AND (
        tb.primaryTitle  = p_title OR
        tb.originalTitle = p_title OR
        EXISTS (SELECT 1 FROM akas a2 
                WHERE a2.titleId = tb.tconst AND a2.title = p_title)
      )
  )
  ORDER BY a.titleId, a.ordering;

  /* ===================== 3) Crew (directores / escritores) ===================== */
  SELECT
    tb.tconst,
    (SELECT GROUP_CONCAT(DISTINCT nb.primaryName ORDER BY nb.primaryName SEPARATOR ', ')
       FROM crew_directors cd
       JOIN name_basics nb ON nb.nconst = cd.nconst
       WHERE cd.tconst = tb.tconst) AS directors,
    (SELECT GROUP_CONCAT(DISTINCT nb.primaryName ORDER BY nb.primaryName SEPARATOR ', ')
       FROM crew_writers cw
       JOIN name_basics nb ON nb.nconst = cw.nconst
       WHERE cw.tconst = tb.tconst) AS writers
  FROM title_basics tb
  WHERE tb.primaryTitle  = p_title
     OR tb.originalTitle = p_title
     OR EXISTS (SELECT 1 FROM akas a 
                WHERE a.titleId = tb.tconst AND a.title = p_title)
  ORDER BY tb.tconst;

  /* ===================== 4) Principals (reparto/equipo) ===================== */
  SELECT
    p.tconst,
    p.ordering,
    p.category,
    p.job,
    p.characters,
    p.nconst,
    nb.primaryName,
    (SELECT GROUP_CONCAT(np.profession ORDER BY np.profession SEPARATOR ', ')
       FROM name_professions np
       WHERE np.nconst = p.nconst) AS professions
  FROM principals p
  JOIN name_basics nb ON nb.nconst = p.nconst
  WHERE EXISTS (
    SELECT 1 FROM title_basics tb
    WHERE tb.tconst = p.tconst
      AND (
        tb.primaryTitle  = p_title OR
        tb.originalTitle = p_title OR
        EXISTS (SELECT 1 FROM akas a 
                WHERE a.titleId = tb.tconst AND a.title = p_title)
      )
  )
  ORDER BY p.tconst, p.ordering;

  /* ===================== 5) Episodios (si es serie) ===================== */
  SELECT
    e.parentTconst        AS series_tconst,
    s.primaryTitle        AS seriesTitle,
    e.seasonNumber,
    e.episodeNumber,
    e.tconst              AS episode_tconst,
    ep.primaryTitle       AS episodeTitle,
    r.averageRating       AS episodeRating,
    r.numVotes            AS episodeVotes
  FROM episodes e
  JOIN title_basics s  ON s.tconst  = e.parentTconst
  JOIN title_basics ep ON ep.tconst = e.tconst
  LEFT JOIN ratings r  ON r.tconst  = e.tconst
  WHERE EXISTS (
    SELECT 1 FROM title_basics tb
    WHERE tb.tconst = e.parentTconst
      AND (
        tb.primaryTitle  = p_title OR
        tb.originalTitle = p_title OR
        EXISTS (SELECT 1 FROM akas a 
                WHERE a.titleId = tb.tconst AND a.title = p_title)
      )
  )
  ORDER BY e.parentTconst, e.seasonNumber, e.episodeNumber;
END;


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

