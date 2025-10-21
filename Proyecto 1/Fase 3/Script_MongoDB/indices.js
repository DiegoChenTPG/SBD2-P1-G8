use bases2_proyecto

// ============= INDICES =============

// 1. Para el que recibe nombre de pelicula, serie o corto y despliega toda su info relacionada
db.title_basics.createIndex({ primaryTitle: "text", originalTitle: "text" }) // búsqueda por nombre
db.ratings.createIndex({ tconst: 1 })
db.basics_genres.createIndex({ tconst: 1 })
db.akas.createIndex({ titleId: 1 })
db.aka_types.createIndex({ titleId: 1, ordering: 1 })
db.aka_attributes.createIndex({ titleId: 1, ordering: 1 })
db.principals.createIndex({ tconst: 1, ordering: 1 })
db.name_basics.createIndex({ nconst: 1 })
db.episodes.createIndex({ parentTconst: 1 })

db.principals.createIndex({ tconst: 1, ordering: 1 }, { unique: true })
db.principals.createIndex({ tconst: 1 })
db.principals.createIndex({ nconst: 1 })



db.title_basics.createIndex(
    { primaryTitle: "text", originalTitle: "text" },   // (A) Búsqueda por palabras (recomendado)
    { name: "txt_titles" }
);
// (B) Si prefieres prefijo/ordenamiento alfabético
db.title_basics.createIndex({ primaryTitle: 1 }, { name: "idx_title_primaryTitle" });
db.title_basics.createIndex({ originalTitle: 1 }, { name: "idx_title_originalTitle" });

// Joins rápidos por clave
db.ratings.createIndex({ tconst: 1 }, { name: "idx_ratings_tconst" });
db.basics_genres.createIndex({ tconst: 1, genre: 1 }, { name: "cov_bg_tconst_genre" }); // cubriente

db.crew_directors.createIndex({ tconst: 1, nconst: 1 }, { unique: true, name: "uniq_cd_pair" });
db.crew_writers.createIndex({ tconst: 1, nconst: 1 }, { unique: true, name: "uniq_cw_pair" });
db.crew_directors.createIndex({ tconst: 1 }, { name: "idx_cd_tconst" });
db.crew_writers.createIndex({ tconst: 1 }, { name: "idx_cw_tconst" });

db.name_basics.createIndex({ nconst: 1, primaryName: 1 }, { name: "cov_name_nconst_primaryName" }); // cubriente

// Episodios (agrupación por temporada sin cargar todo)
db.episodes.createIndex({ parentTconst: 1, seasonNumber: 1 }, { name: "idx_ep_parent_season" });



// 2. Para las peliculas de un director
// nombres (resolución de nconst por nombre)
db.name_basics.createIndex({ primaryName: 1 }, { name: "idx_nb_primaryName" });
// relaciones director→título
db.crew_directors.createIndex({ nconst: 1, tconst: 1 }, { name: "idx_cd_nconst_tconst" });
// títulos (join + filtro por tipo + orden)
db.title_basics.createIndex({ tconst: 1 }, { name: "pk_title_basics" });
db.title_basics.createIndex({ titleType: 1, startYear: 1, primaryTitle: 1 }, { name: "idx_tb_type_year_title" });

// 3. Para las Top 10 peliculas con mejor rating

db.title_basics.createIndex({ titleType: 1 }, { name: "idx_tb_titleType" });
db.ratings.createIndex({ tconst: 1 }, { name: "pk_ratings_tconst" });
db.ratings.createIndex({ averageRating: -1, numVotes: -1 }, { name: "idx_ratings_rating_votes" });
db.title_basics.createIndex({ tconst: 1, titleType: 1 }, { name: "idx_tb_tconst_type" });

// 4. Para el director con las peliculas
db.title_basics.createIndex({ titleType: 1, tconst: 1 }, { name: "idx_tb_type_tconst" });
db.crew_directors.createIndex({ tconst: 1, nconst: 1 }, { name: "idx_cd_tconst_nconst" });
db.name_basics.createIndex({ nconst: 1 }, { name: "pk_nb_nconst" });
db.material_director_movie_counts.createIndex({ peliculas: -1, director: 1 }, { name: "idx_mdc_peliculas_director" })

// 5. Para el top 10 actores con mas peliculas
db.material_actor_movie_counts.createIndex({ peliculas: -1, actor: 1 }, { name: "idx_mamc_peliculas_actor" });








