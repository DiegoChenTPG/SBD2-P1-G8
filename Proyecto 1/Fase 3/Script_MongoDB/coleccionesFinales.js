// Esto es por si las otras se tardan demasiado, al final se uso este
use bases2_proyecto;

// 1. Informacion de pelicula, serie o corto por el nombre del titulo
db.title_basics.aggregate([
    // ratings (solo lo necesario)
    {
        $lookup: {
            from: "ratings",
            localField: "tconst",
            foreignField: "tconst",
            pipeline: [{ $project: { _id: 0, averageRating: 1, numVotes: 1 } }],
            as: "rating"
        }
    },
    { $set: { rating: { $first: "$rating" } } },

    // géneros (solo nombres)
    {
        $lookup: {
            from: "basics_genres",
            localField: "tconst",
            foreignField: "tconst",
            pipeline: [{ $project: { _id: 0, genre: 1 } }],
            as: "genres_docs"
        }
    },
    { $set: { genres: { $map: { input: "$genres_docs", as: "g", in: "$$g.genre" } } } },

    // resumen de episodios por temporada (sin traer todo el array)
    {
        $lookup: {
            from: "episodes",
            let: { t: "$tconst" },
            pipeline: [
                { $match: { $expr: { $eq: ["$parentTconst", "$$t"] } } },
                { $group: { _id: "$seasonNumber", episodesInSeason: { $sum: 1 } } },
                { $sort: { _id: 1 } },
                {
                    $group: {
                        _id: null,
                        breakdown: { $push: { seasonNumber: "$_id", episodesInSeason: "$episodesInSeason" } },
                        seasonsCount: { $sum: 1 },
                        episodesCount: { $sum: "$episodesInSeason" }
                    }
                },
                { $project: { _id: 0, breakdown: 1, seasonsCount: 1, episodesCount: 1 } }
            ],
            as: "epinfo"
        }
    },
    {
        $set: {
            episodesBySeason: { $ifNull: [{ $first: "$epinfo.breakdown" }, []] },
            seasonsCount: { $ifNull: [{ $first: "$epinfo.seasonsCount" }, 0] },
            episodesCount: { $ifNull: [{ $first: "$epinfo.episodesCount" }, 0] }
        }
    },

    // documento final
    {
        $project: {
            _id: 0,
            tconst: 1, titleType: 1, primaryTitle: 1, originalTitle: 1,
            isAdult: 1, startYear: 1, runtimeMinutes: 1,
            genres: 1,
            rating: 1,
            seasonsCount: 1,
            episodesCount: 1,
            episodesBySeason: 1
        }
    },

    // Materializa: inserta si no existe, reemplaza si ya existe
    { $merge: { into: "titles_view", whenMatched: "replace", whenNotMatched: "insert" } }
], { allowDiskUse: true });

// Actualizar con directores y escritores en la vista materializada
db.titles_view.aggregate([
    // Solo los que aún no tienen directors o writers
    {
        $match: {
            $or: [
                { directors: { $exists: false } },
                { writers: { $exists: false } }
            ]
        }
    },

    // DIRECTORES
    {
        $lookup: {
            from: "crew_directors",
            localField: "tconst",
            foreignField: "tconst",
            as: "_dir_links"
        }
    },
    {
        $lookup: {
            from: "name_basics",
            localField: "_dir_links.nconst",
            foreignField: "nconst",
            as: "_dir_people"
        }
    },
    {
        $set: {
            directors: { $map: { input: "$_dir_people", as: "p", in: "$$p.primaryName" } }
        }
    },
    { $unset: ["_dir_links", "_dir_people"] },

    // WRITERS
    {
        $lookup: {
            from: "crew_writers",
            localField: "tconst",
            foreignField: "tconst",
            as: "_wri_links"
        }
    },
    {
        $lookup: {
            from: "name_basics",
            localField: "_wri_links.nconst",
            foreignField: "nconst",
            as: "_wri_people"
        }
    },
    {
        $set: {
            writers: { $map: { input: "$_wri_people", as: "p", in: "$$p.primaryName" } }
        }
    },
    { $unset: ["_wri_links", "_wri_people"] },

    // Escribe de vuelta en la vista materializada
    {
        $merge: {
            into: "titles_view",
            on: "tconst",
            whenMatched: "merge",
            whenNotMatched: "discard"
        }
    }
], { allowDiskUse: true });

// Patch para el tema de los episodios
db.titles_view.aggregate([
    // Solo títulos que son series/miniseries
    { $match: { titleType: { $in: ["tvSeries", "tvMiniSeries"] } } },
    { $project: { _id: 0, tconst: 1 } },

    // Resumen de episodios por temporada
    {
        $lookup: {
            from: "episodes",
            let: { t: "$tconst" },
            pipeline: [
                { $match: { $expr: { $eq: ["$parentTconst", "$$t"] } } },
                { $group: { _id: "$seasonNumber", episodesInSeason: { $sum: 1 } } },
                { $sort: { _id: 1 } },
                {
                    $group: {
                        _id: null,
                        breakdown: { $push: { seasonNumber: "$_id", episodesInSeason: "$episodesInSeason" } },
                        seasonsCount: { $sum: 1 },
                        episodesCount: { $sum: "$episodesInSeason" }
                    }
                },
                { $project: { _id: 0, breakdown: 1, seasonsCount: 1, episodesCount: 1 } }
            ],
            as: "epinfo"
        }
    },

    // Si no hay episodios, deja valores en 0/[]
    { $unwind: { path: "$epinfo", preserveNullAndEmptyArrays: true } },
    {
        $set: {
            episodesBySeason: { $ifNull: ["$epinfo.breakdown", []] },
            seasonsCount: { $ifNull: ["$epinfo.seasonsCount", 0] },
            episodesCount: { $ifNull: ["$epinfo.episodesCount", 0] }
        }
    },
    { $project: { epinfo: 0 } },

    // Escribir de vuelta SOLO donde ya existe el tconst en titles_view
    { $merge: { into: "titles_view", on: "tconst", whenMatched: "merge", whenNotMatched: "discard" } }
], { allowDiskUse: true });

db.titles_view.find({ primaryTitle: "Gladiator" }).toArray(); // de esta manera se usa cuando no esta dentro de una funcion




// 4. Director con mas peliculas
function rebuildMaterialDirectorMovies() {
    db.title_basics.aggregate([
        // Solo películas
        { $match: { titleType: "movie" } },
        { $project: { _id: 0, tconst: 1 } },

        // Directores por película (deduplicados dentro del lookup)
        {
            $lookup: {
                from: "principals",
                let: { t: "$tconst" },
                pipeline: [
                    {
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: ["$tconst", "$$t"] },
                                    { $eq: ["$category", "director"] }
                                ]
                            }
                        }
                    },
                    { $group: { _id: "$nconst" } },              // nconst únicos por tconst
                    { $project: { _id: 0, nconst: "$_id" } }
                ],
                as: "dirs"
            }
        },
        { $unwind: "$dirs" },

        // Conteo de películas por director (nconst)
        { $group: { _id: "$dirs.nconst", peliculas: { $sum: 1 } } },

        // Resolver nombre del director
        { $lookup: { from: "name_basics", localField: "_id", foreignField: "nconst", as: "nb" } },
        { $unwind: "$nb" },

        // Documento final: _id = nconst  (esto hace único el join para $merge)
        { $project: { _id: 1, director: "$nb.primaryName", peliculas: 1 } },

        // Escribir/actualizar (no necesitamos 'on' porque usa _id)
        { $merge: { into: "material_director_movie_counts", whenMatched: "replace", whenNotMatched: "insert" } }
    ], { allowDiskUse: true, maxTimeMS: 0 });
}
// Ejecutar en dando caso se cambie la data, si no solo 1 ves
rebuildMaterialDirectorMovies();


// 5. Top 10 actores con mas peliculas
function rebuildMaterialActorMovies() {
    db.title_basics.aggregate([
        // Solo películas
        { $match: { titleType: "movie" } },
        { $project: { _id: 0, tconst: 1 } },

        // Actores por película (deduplicados dentro del lookup)
        {
            $lookup: {
                from: "principals",
                let: { t: "$tconst" },
                pipeline: [
                    {
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: ["$tconst", "$$t"] },
                                    { $in: ["$category", ["actor", "actress"]] }
                                ]
                            }
                        }
                    },
                    { $group: { _id: "$nconst" } },             // nconst únicos por tconst
                    { $project: { _id: 0, nconst: "$_id" } }
                ],
                as: "cast"
            }
        },
        { $unwind: "$cast" },

        // Conteo de películas por actor (nconst)
        { $group: { _id: "$cast.nconst", peliculas: { $sum: 1 } } },

        // Adjuntar nombre del actor
        { $lookup: { from: "name_basics", localField: "_id", foreignField: "nconst", as: "nb" } },
        { $unwind: "$nb" },

        // Documento final: _id = nconst para merge sin índice extra
        { $project: { _id: 1, actor: "$nb.primaryName", peliculas: 1 } },

        // Escribir/actualizar colección materializada
        { $merge: { into: "material_actor_movie_counts", whenMatched: "replace", whenNotMatched: "insert" } }
    ], { allowDiskUse: true, maxTimeMS: 0 });
}

// Ejecutar en dando caso se cambie la data, si no solo 1 ves
rebuildMaterialActorMovies();





