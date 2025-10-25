use bases2_proyecto;
// RECORDAR DE RE CREAR LAS FUNCIONES CADA VES QUE SE REINICIE LA BASE DE DATOS, SOLO LAS FUNCIONES
// 1. Informacion de pelicula, serie o corto por el nombre del titulo
// coincide por texto (contiene, case-insensitive) y se le pueden poner limites 

function InfoTitulo(nombre) {
    if (!nombre || typeof nombre !== "string") {
        throw new Error("Debes enviar el nombre del título (string).");
    }
    return db.titles_view.find({ primaryTitle: nombre }).toArray()
}

InfoTitulo("Gladiator")
/*
const obtenerInfoTitulo = (texto, limite = null) => {
    const pipeline = [
        {
            $match: {
                $or: [
                    { primaryTitle: { $regex: texto, $options: "i" } },
                    { originalTitle: { $regex: texto, $options: "i" } }
                ]
            }
        },

        // (el $limit se insertará dinámicamente si procede)

        { $lookup: { from: "ratings", localField: "tconst", foreignField: "tconst", as: "rating" } },
        { $unwind: { path: "$rating", preserveNullAndEmptyArrays: true } },

        { $lookup: { from: "basics_genres", localField: "tconst", foreignField: "tconst", as: "genres_docs" } },
        { $addFields: { genres: { $map: { input: "$genres_docs", as: "g", in: "$$g.genre" } } } },

        { $lookup: { from: "crew_directors", localField: "tconst", foreignField: "tconst", as: "dir_links" } },
        { $lookup: { from: "name_basics", localField: "dir_links.nconst", foreignField: "nconst", as: "directors" } },
        { $lookup: { from: "crew_writers", localField: "tconst", foreignField: "tconst", as: "wri_links" } },
        { $lookup: { from: "name_basics", localField: "wri_links.nconst", foreignField: "nconst", as: "writers" } },

        { $lookup: { from: "episodes", localField: "tconst", foreignField: "parentTconst", as: "episodes" } },

        {
            $set: {
                _episodesSafe: { $ifNull: ["$episodes", []] },
                _seasonsList: {
                    $filter: {
                        input: { $map: { input: { $ifNull: ["$episodes", []] }, as: "e", in: "$$e.seasonNumber" } },
                        as: "s",
                        cond: { $ne: ["$$s", null] }
                    }
                }
            }
        },
        {
            $set: {
                seasonsCount: { $size: { $setUnion: [[], "$_seasonsList"] } },
                episodesCount: { $size: "$_episodesSafe" }
            }
        },

        {
            $facet: {
                keep: [
                    {
                        $project: {
                            _id: 0,
                            tconst: 1, titleType: 1, primaryTitle: 1, originalTitle: 1,
                            isAdult: 1, startYear: 1, endYear: 1, runtimeMinutes: 1,
                            genres: 1,
                            rating: { average: "$rating.averageRating", votes: "$rating.numVotes" },
                            directors: "$directors.primaryName",
                            writers: "$writers.primaryName",
                            seasonsCount: 1,
                            episodesCount: 1
                        }
                    }
                ],
                bySeason: [
                    { $unwind: { path: "$episodes", preserveNullAndEmptyArrays: false } },
                    { $group: { _id: { t: "$tconst", s: "$episodes.seasonNumber" }, episodesInSeason: { $sum: 1 } } },
                    { $sort: { "_id.s": 1 } },
                    { $group: { _id: "$_id.t", breakdown: { $push: { seasonNumber: "$_id.s", episodesInSeason: "$episodesInSeason" } } } }
                ]
            }
        },

        {
            $project: {
                items: {
                    $map: {
                        input: "$keep",
                        as: "k",
                        in: {
                            $mergeObjects: [
                                "$$k",
                                {
                                    episodesBySeason: {
                                        $let: {
                                            vars: {
                                                b: {
                                                    $first: {
                                                        $filter: { input: "$bySeason", as: "br", cond: { $eq: ["$$br._id", "$$k.tconst"] } }
                                                    }
                                                }
                                            },
                                            in: { $ifNull: ["$$b.breakdown", []] }
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        },
        { $unwind: "$items" },
        { $replaceWith: "$items" }
    ];

    // Inserta $limit después del $match solo si mandas un límite > 0
    if (Number.isFinite(limite) && limite > 0) {
        pipeline.splice(1, 0, { $limit: limite });
    }

    return db.title_basics.aggregate(pipeline).toArray();
};

// se usa asi 
obtenerInfoTitulo("Breaking Bad");
*/

// 2. Peliculas de un director por su nombre
function peliculasDeDirectorViejo(nombre) {
    if (!nombre || typeof nombre !== "string") {
        throw new Error("Debes enviar un nombre de director (string).");
    }

    // Igualdad exacta para aprovechar índice (evita collation costosa).
    // Pasa el nombre tal cual está en name_basics.primaryName.
    const nconsts = db.name_basics
        .find({ primaryName: nombre }, { _id: 0, nconst: 1 })
        .toArray()
        .map(d => d.nconst);

    if (!nconsts.length) return [];

    return db.crew_directors.aggregate([
        // 1) tomar solo filas del/los directores solicitados
        { $match: { nconst: { $in: nconsts } } },

        // 2) tconst únicos (reduce el set antes del join)
        { $group: { _id: "$tconst" } },

        // 3) lookup con pipeline: filtra dentro del join a SOLO "movie" y proyecta lo mínimo
        {
            $lookup: {
                from: "title_basics",
                let: { t: "$_id" },
                pipeline: [
                    {
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: ["$tconst", "$$t"] },
                                    { $eq: ["$titleType", "movie"] }
                                ]
                            }
                        }
                    },
                    { $project: { _id: 0, tconst: 1, primaryTitle: 1, startYear: 1, titleType: 1 } }
                ],
                as: "tb"
            }
        },

        // 4) descarta los que no sean "movie" (tb vacío)
        { $unwind: "$tb" },

        // 5) salida ordenada
        { $replaceRoot: { newRoot: "$tb" } },
        { $sort: { startYear: 1, primaryTitle: 1 } }
    ], { allowDiskUse: true }).toArray();
}

// se usa asi
peliculasDeDirectorViejo("Christopher Nolan");


// 3. Top 10 peliculas con mejor rating
function top10PeliculasMejorRatingViejo() {
    return db.ratings.aggregate([
        // Orden global usando el índice {averageRating:-1, numVotes:-1}
        { $sort: { averageRating: -1, numVotes: -1 } },

        // Unir a title_basics filtrando SOLO películas en el pipeline del lookup
        {
            $lookup: {
                from: "title_basics",
                let: { t: "$tconst" },
                pipeline: [
                    {
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: ["$tconst", "$$t"] },
                                    { $eq: ["$titleType", "movie"] }
                                ]
                            }
                        }
                    },
                    { $project: { _id: 0, tconst: 1, primaryTitle: 1, startYear: 1, titleType: 1 } }
                ],
                as: "tb"
            }
        },

        // Deja pasar solo los que sí son "movie"
        { $unwind: "$tb" },

        // Proyección final
        {
            $project: {
                _id: 0,
                tconst: 1,
                averageRating: 1,
                numVotes: 1,
                primaryTitle: "$tb.primaryTitle",
                startYear: "$tb.startYear",
                titleType: "$tb.titleType"
            }
        },

        // Top 10 final
        { $limit: 10 }
    ], { allowDiskUse: true }).toArray();
}

top10PeliculasMejorRatingViejo();


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

function directorConMasPeliculas() {
    const doc = db.material_director_movie_counts
        .find({}, { _id: 0, director: 1, peliculas: 1 })
        .sort({ peliculas: -1, director: 1 })
        .limit(1)
        .toArray();
    return doc[0] || null;
}

directorConMasPeliculas() // esta es la funcion que se construye en parte gracias a la de arriba


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

function top10ActoresConMasPeliculas() {
    return db.material_actor_movie_counts
        .find({}, { _id: 0, actor: 1, peliculas: 1 })
        .sort({ peliculas: -1, actor: 1 })
        .limit(10)
        .toArray();
}

top10ActoresConMasPeliculas()

