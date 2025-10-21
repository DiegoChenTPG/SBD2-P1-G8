use bases2_proyecto;

// Construccion mas rapida de 1. title_views
db.ratings.createIndex({ tconst: 1 }, { name: "idx_ratings_tconst" });
db.basics_genres.createIndex({ tconst: 1, genre: 1 }, { name: "cov_bg_tconst_genre" });
db.episodes.createIndex({ parentTconst: 1, seasonNumber: 1 }, { name: "idx_ep_parent_season" });
// Para consultar 1. title_views
db.titles_view.createIndex({ tconst: 1 }, { unique: true, name: "idx_tv_tconst" });
db.titles_view.createIndex({ primaryTitle: 1 }, { name: "idx_tv_primaryTitle" });
// (opcional si harás búsquedas por palabras en la vista)
db.titles_view.createIndex({ primaryTitle: "text", originalTitle: "text" }, { name: "txt_tv_titles" });
db.episodes.createIndex({ parentTconst: 1, seasonNumber: 1 }, { name: "idx_ep_parent_season" });





