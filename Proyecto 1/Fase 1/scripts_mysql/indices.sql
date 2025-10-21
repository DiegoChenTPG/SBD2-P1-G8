-- Creados para mejorar el rendimiento de las consultas en la base de datos
-- ============================================================================
-- Fueron creados despues de la carga masiva, ya que estos aumentan la velocidad de lectura con los SELECT
-- pero disminuyen la velocidad de escritura (INSERT, UPDATE, DELETE), cosa que hubiera hecho que la carga de datos en ves de casi 12 horas
-- se hubiera tomado mucho mas tiempo
-- ============================================================================

-- En dado caso creamos que son necesarios mas indices, se ira actualizando este archivo

-- Titulos por nombre exacto
CREATE INDEX idx_tb_primaryTitle ON title_basics (primaryTitle(255));
CREATE INDEX idx_tb_originalTitle ON title_basics (originalTitle(255));

CREATE INDEX idx_akas_title ON akas (title(255));

-- Por nombre de persona
CREATE INDEX idx_nb_primaryName ON name_basics (primaryName);

-- Por serie y ordenado
CREATE INDEX idx_ep_parent ON episodes (parentTconst);
CREATE INDEX idx_ep_parent_s_e ON episodes (parentTconst, seasonNumber, episodeNumber);

-- Por genero
CREATE INDEX idx_bg_genre ON basics_genres (genre, tconst);

-- Por rating y votos promedio
CREATE INDEX idx_rt_numVotes ON ratings (numVotes);
CREATE INDEX idx_rt_avgRating ON ratings (averageRating);

-- Filtro por tipo y año 
CREATE INDEX idx_tb_titleType ON title_basics (titleType);
CREATE INDEX idx_tb_startYear ON title_basics (startYear);


-- Este es enfocado para el cuarto procedimiento Top 10 actores/actrices con mas peliculas
-- Filtra primero por category y agrupa/une eficiente por nconst
CREATE INDEX idx_p_cat_nconst_tconst ON principals (category, nconst, tconst);
-- Usar de nuevo el mismo de arriba para “solo movies” CREATE INDEX idx_tb_type_tconst ON title_basics (titleType, tconst);



