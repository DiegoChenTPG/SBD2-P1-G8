CREATE DATABASE IF NOT EXISTS BASES2_PROYECTOS;
USE BASES2_PROYECTOS;

-- ============================================================================
-- TITLE BASICS
-- Clave lógica: tconst (identificador del título)
-- Relaciones conceptuales:
--   - basics_genres.tconst  -> title_basics.tconst
--   - akas.titleId          -> title_basics.tconst
--   - episodes.tconst       -> title_basics.tconst
--   - episodes.parentTconst -> title_basics.tconst
--   - principals.tconst     -> title_basics.tconst
--   - ratings.tconst        -> title_basics.tconst
--   - name_known_for.tconst -> title_basics.tconst
-- ============================================================================
CREATE TABLE title_basics (
    tconst         VARCHAR(20) PRIMARY KEY,
    titleType      VARCHAR(64)  NOT NULL,
    primaryTitle   VARCHAR(1024) NOT NULL,
    originalTitle  VARCHAR(1024) NOT NULL,
    isAdult        TINYINT       NOT NULL,  -- 0/1
    startYear     SMALLINT     NULL,      -- YYYY; usar NULL si venía "\N"
    endYear       SMALLINT     NULL,      -- YYYY; usar NULL si venía "\N"
    runtimeMinutes INT           NULL       -- usar NULL si venía "\N"
);

-- ============================================================================
-- BASICS GENRES (normalización de géneros)
-- Clave lógica: (tconst, genre)
-- Relación: tconst -> title_basics.tconst
-- ============================================================================
CREATE TABLE basics_genres (
    tconst       VARCHAR(20)  NOT NULL,
    primaryTitle VARCHAR(1024) NOT NULL, 
    genre        VARCHAR(64)  NOT NULL,
    PRIMARY KEY (tconst, genre),
    FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
);

-- ============================================================================
-- AKAS (títulos alternativos)
-- Clave lógica: (titleId, ordering)
-- Relación: titleId -> title_basics.tconst
-- ============================================================================
CREATE TABLE akas (
    titleId         VARCHAR(20)  NOT NULL,
    ordering        INT          NOT NULL,
    title           VARCHAR(1024) NOT NULL,
    region          VARCHAR(64)  NULL,
    isOriginalTitle TINYINT      NOT NULL, -- 0/1
    PRIMARY KEY (titleId, ordering),
    FOREIGN KEY (titleId) REFERENCES title_basics(tconst)
);

-- ============================================================================
-- AKA TYPES (normalización de types por AKA)
-- Clave lógica: (titleId, ordering, type)
-- Relación: (titleId, ordering) -> akas(titleId, ordering)
-- ============================================================================
CREATE TABLE aka_types (
    titleId  VARCHAR(20) NOT NULL,
    ordering INT         NOT NULL,
    type     VARCHAR(64) NOT NULL,
    PRIMARY KEY (titleId, ordering, type),
    FOREIGN KEY (titleId, ordering) REFERENCES akas(titleId, ordering)
);

-- ============================================================================
-- AKA ATTRIBUTES (normalización de attributes por AKA)
-- Clave lógica: (titleId, ordering, attribute)
-- Relación: (titleId, ordering) -> akas(titleId, ordering)
-- ============================================================================
CREATE TABLE aka_attributes (
    titleId   VARCHAR(20)  NOT NULL,
    ordering  INT          NOT NULL,
    attribute VARCHAR(256) NOT NULL,
    PRIMARY KEY (titleId, ordering, attribute),
    FOREIGN KEY (titleId, ordering) REFERENCES akas(titleId, ordering)
);

-- ============================================================================
-- NAME BASICS (personas)
-- Clave lógica: nconst
-- Relaciones conceptuales:
--   - crew_directors.nconst   -> name_basics.nconst
--   - crew_writers.nconst     -> name_basics.nconst
--   - principals.nconst       -> name_basics.nconst
--   - name_professions.nconst -> name_basics.nconst
--   - name_known_for.nconst   -> name_basics.nconst
-- ============================================================================
CREATE TABLE name_basics (
    nconst      VARCHAR(20) PRIMARY KEY,
    primaryName VARCHAR(512) NOT NULL,
    birthYear  SMALLINT    NULL,          -- YYYY (NULL si \N)
    deathYear  SMALLINT    NULL           -- YYYY (NULL si \N)
);

-- ============================================================================
-- CREW - DIRECTORS (relación muchos-a-muchos)
-- Clave lógica: (tconst, nconst)
-- Relaciones:
--   - tconst -> title_basics.tconst
--   - nconst -> name_basics.nconst
-- ============================================================================
CREATE TABLE crew_directors (
    tconst VARCHAR(20) NOT NULL,
    nconst VARCHAR(20) NOT NULL,
    PRIMARY KEY (tconst, nconst),
    FOREIGN KEY (tconst) REFERENCES title_basics(tconst),
    FOREIGN KEY (nconst) REFERENCES name_basics(nconst)
);

-- ============================================================================
-- CREW - WRITERS (relación muchos-a-muchos)
-- Clave lógica: (tconst, nconst)
-- Relaciones:
--   - tconst -> title_basics.tconst
--   - nconst -> name_basics.nconst
-- ============================================================================
CREATE TABLE crew_writers (
    tconst VARCHAR(20) NOT NULL,
    nconst VARCHAR(20) NOT NULL,
    PRIMARY KEY (tconst, nconst),
    FOREIGN KEY (tconst) REFERENCES title_basics(tconst),
    FOREIGN KEY (nconst) REFERENCES name_basics(nconst)
);

-- ============================================================================
-- EPISODES
-- Clave lógica: tconst (episodio)
-- Relaciones:
--   - tconst       -> title_basics.tconst (episodio)
--   - parentTconst -> title_basics.tconst (serie/padre)
-- ============================================================================
CREATE TABLE episodes (
    tconst        VARCHAR(20) PRIMARY KEY,
    parentTconst  VARCHAR(20) NOT NULL,
    seasonNumber  INT         NULL,
    episodeNumber INT         NULL,
    FOREIGN KEY (tconst)       REFERENCES title_basics(tconst),
    FOREIGN KEY (parentTconst) REFERENCES title_basics(tconst)
);

-- ============================================================================
-- PRINCIPALS
-- Clave lógica: (tconst, ordering)
-- Relaciones:
--   - tconst -> title_basics.tconst
--   - nconst -> name_basics.nconst
-- Notas:
--   - job y characters pueden venir como "\N" (almacenar NULL)
-- ============================================================================
CREATE TABLE principals (
    tconst    VARCHAR(20) NOT NULL,
    ordering  INT         NOT NULL,
    nconst    VARCHAR(20) NOT NULL,
    category  VARCHAR(64) NOT NULL,
    job       VARCHAR(512) NULL,
    characters TEXT        NULL,
    PRIMARY KEY (tconst, ordering),
    FOREIGN KEY (tconst) REFERENCES title_basics(tconst),
    FOREIGN KEY (nconst) REFERENCES name_basics(nconst)
);

-- ============================================================================
-- RATINGS
-- Clave lógica: tconst
-- Relación: tconst -> title_basics.tconst
-- ============================================================================
CREATE TABLE ratings (
    tconst        VARCHAR(20) PRIMARY KEY,
    averageRating DECIMAL(3,1) NOT NULL,  -- p.ej. 7.8
    numVotes      INT          NOT NULL,
    FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
);

-- ============================================================================
-- NAME PROFESSIONS (normalización de primaryProfession)
-- Clave lógica: (nconst, profession)
-- Relación: nconst -> name_basics.nconst
-- ============================================================================
CREATE TABLE name_professions (
    nconst     VARCHAR(20) NOT NULL,
    profession VARCHAR(64) NOT NULL,
    PRIMARY KEY (nconst, profession),
    FOREIGN KEY (nconst) REFERENCES name_basics(nconst)
);

-- ============================================================================
-- NAME KNOWN FOR (normalización de knownForTitles)
-- Clave lógica: (nconst, tconst)
-- Relaciones:
--   - nconst -> name_basics.nconst
--   - tconst -> title_basics.tconst
-- ============================================================================
CREATE TABLE name_known_for (
    nconst VARCHAR(20) NOT NULL,
    tconst VARCHAR(20) NOT NULL,
    PRIMARY KEY (nconst, tconst),
    FOREIGN KEY (nconst) REFERENCES name_basics(nconst),
    FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
);

-- ============================================================================
-- Notas para el que se va a encargar de la carga masiva
-- ============================================================================
-- 1) Inserta primero:
--      - title_basics
--      - name_basics
--    Luego las tablas dependientes (genres, akas, crew, episodes, principals, ratings, etc.)
--
-- 2) Convierte '\N' a NULL antes de insertar (LOAD DATA o proceso ETL).
--
-- 3) Campos booleanos (isAdult, isOriginalTitle): se usa 0/1.
--
-- 4) Listas separadas por comas (genres, types, attributes, professions, knownForTitles):
--    dividir en tu proceso ETL y llenar las tablas normalizadas correspondientes.
