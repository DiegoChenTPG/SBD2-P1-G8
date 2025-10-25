// ESTAS FUNCIONES UNICAMENTE HACEN LLAMADAS A LAS VISTAS-COLECCIONES CREADAS 
use bases2_proyecto;

//1.  SALE A BASE DE titles_view
function InfoTitulo(nombre) {
    if (!nombre || typeof nombre !== "string") {
        throw new Error("Debes enviar el nombre del título (string).");
    }
    return db.titles_view.find({ primaryTitle: nombre }).toArray()
}

InfoTitulo("Gladiator")

// 2. SALE A BASE DE titles_view
function peliculasDeDirector(nombreDirector) {
    return db.titles_view.find(
        { titleType: "movie", directors: { $in: [nombreDirector] } }, // match exacto
        { _id: 0, tconst: 1, primaryTitle: 1, startYear: 1, titleType: 1 }
    )
        .sort({ startYear: 1, primaryTitle: 1 })
        .toArray();
}

peliculasDeDirector("Christopher Nolan");


// 3. SALE A BASE DE titles_view
function top10PeliculasMejorRating() {
    return db.titles_view.aggregate([
        { $match: { titleType: "movie", "rating.averageRating": { $type: "number" } } },
        { $sort: { "rating.averageRating": -1, "rating.numVotes": -1 } },
        { $limit: 10 },
        {
            $project: {
                _id: 0,
                primaryTitle: 1,
                startYear: 1,
                titleType: 1,
                averageRating: { $toDouble: "$rating.averageRating" },
                numVotes: { $toDouble: "$rating.numVotes" }
            }
        }
    ]).toArray();
}

top10PeliculasMejorRating()


// 4. SALE A BASE DE material_director_movie_counts
function directorConMasPeliculas() {
    return db.material_director_movie_counts
        .find({}, { _id: 0, director: 1, peliculas: 1 })
        .sort({ peliculas: -1, director: 1 })
        .limit(1)
        .toArray();
}

directorConMasPeliculas()

// 5. SALE A BASE DE material_actor_movie_counts
function top10ActoresConMasPeliculas() {
    return db.material_actor_movie_counts
        .find({}, { _id: 0, actor: 1, peliculas: 1 })
        .sort({ peliculas: -1, actor: 1 })
        .limit(10)
        .toArray();
}

top10ActoresConMasPeliculas()


// 6. 


