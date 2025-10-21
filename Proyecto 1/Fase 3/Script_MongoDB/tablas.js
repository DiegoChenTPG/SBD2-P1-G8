// Usa la BD que prefieras (cámbiala si deseas)
use bases2_proyecto;

/* ============================================================
TITLE BASICS  (tconst PK)
============================================================ */
db.createCollection("title_basics", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["tconst", "titleType", "primaryTitle", "originalTitle", "isAdult"],
            properties: {
                tconst: { bsonType: "string" },
                titleType: { bsonType: "string" },
                primaryTitle: { bsonType: "string" },
                originalTitle: { bsonType: "string" },
                isAdult: { bsonType: "bool" },
                startYear: { bsonType: ["int", "null"] },
                endYear: { bsonType: ["int", "null"] },
                runtimeMinutes: { bsonType: ["int", "null"] }
            }
        }
    }
});
db.title_basics.createIndex({ tconst: 1 }, { unique: true, name: "pk_title_basics" });

/* ============================================================
BASICS GENRES  ((tconst, genre) PK)   FK: tconst → title_basics
   ============================================================ */
db.createCollection("basics_genres", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["tconst", "primaryTitle", "genre"],
            properties: {
                tconst: { bsonType: "string" },
                primaryTitle: { bsonType: "string" },
                genre: { bsonType: "string" }
            }
        }
    }
});
db.basics_genres.createIndex({ tconst: 1, genre: 1 }, { unique: true, name: "pk_basics_genres" });
db.basics_genres.createIndex({ tconst: 1 }, { name: "fk_bg_tconst" });

/* ============================================================
AKAS  ((titleId, ordering) PK)  FK: titleId → title_basics
============================================================ */
db.createCollection("akas", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["titleId", "ordering", "title", "isOriginalTitle"],
            properties: {
                titleId: { bsonType: "string" },
                ordering: { bsonType: "int" },
                title: { bsonType: "string" },
                region: { bsonType: ["string", "null"] },
                isOriginalTitle: { bsonType: "bool" }
            }
        }
    }
});
db.akas.createIndex({ titleId: 1, ordering: 1 }, { unique: true, name: "pk_akas" });
db.akas.createIndex({ titleId: 1 }, { name: "fk_akas_titleId" });

/* ============================================================
AKA TYPES  ((titleId, ordering, type) PK)  FK: (titleId,ordering) → akas
============================================================ */
db.createCollection("aka_types", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["titleId", "ordering", "type"],
            properties: {
                titleId: { bsonType: "string" },
                ordering: { bsonType: "int" },
                type: { bsonType: "string" }
            }
        }
    }
});
db.aka_types.createIndex({ titleId: 1, ordering: 1, type: 1 }, { unique: true, name: "pk_aka_types" });
db.aka_types.createIndex({ titleId: 1, ordering: 1 }, { name: "fk_aka_types_titleId_ordering" });

/* ============================================================
AKA ATTRIBUTES  ((titleId, ordering, attribute) PK)  FK: (titleId,ordering) → akas
============================================================ */
db.createCollection("aka_attributes", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["titleId", "ordering", "attribute"],
            properties: {
                titleId: { bsonType: "string" },
                ordering: { bsonType: "int" },
                attribute: { bsonType: "string" }
            }
        }
    }
});
db.aka_attributes.createIndex({ titleId: 1, ordering: 1, attribute: 1 }, { unique: true, name: "pk_aka_attributes" });
db.aka_attributes.createIndex({ titleId: 1, ordering: 1 }, { name: "fk_aka_attrs_titleId_ordering" });

/* ============================================================
NAME BASICS  (nconst PK)
============================================================ */
db.createCollection("name_basics", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["nconst", "primaryName"],
            properties: {
                nconst: { bsonType: "string" },
                primaryName: { bsonType: "string" },
                birthYear: { bsonType: ["int", "null"] },
                deathYear: { bsonType: ["int", "null"] }
            }
        }
    }
});
db.name_basics.createIndex({ nconst: 1 }, { unique: true, name: "pk_name_basics" });

/* ============================================================
CREW - DIRECTORS ((tconst, nconst) PK)  FK: tconst → title_basics, nconst → name_basics
============================================================ */
db.createCollection("crew_directors", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["tconst", "nconst"],
            properties: {
                tconst: { bsonType: "string" },
                nconst: { bsonType: "string" }
            }
        }
    }
});
db.crew_directors.createIndex({ tconst: 1, nconst: 1 }, { unique: true, name: "pk_crew_directors" });
db.crew_directors.createIndex({ tconst: 1 }, { name: "fk_cd_tconst" });
db.crew_directors.createIndex({ nconst: 1 }, { name: "fk_cd_nconst" });

/* ============================================================
CREW - WRITERS ((tconst, nconst) PK)  FK: tconst → title_basics, nconst → name_basics
============================================================ */
db.createCollection("crew_writers", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["tconst", "nconst"],
            properties: {
                tconst: { bsonType: "string" },
                nconst: { bsonType: "string" }
            }
        }
    }
});
db.crew_writers.createIndex({ tconst: 1, nconst: 1 }, { unique: true, name: "pk_crew_writers" });
db.crew_writers.createIndex({ tconst: 1 }, { name: "fk_cw_tconst" });
db.crew_writers.createIndex({ nconst: 1 }, { name: "fk_cw_nconst" });

/* ============================================================
EPISODES (tconst PK)  FK: parentTconst → title_basics
============================================================ */
db.createCollection("episodes", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["tconst", "parentTconst"],
            properties: {
                tconst: { bsonType: "string" },
                parentTconst: { bsonType: "string" },
                seasonNumber: { bsonType: ["int", "null"] },
                episodeNumber: { bsonType: ["int", "null"] }
            }
        }
    }
});
db.episodes.createIndex({ tconst: 1 }, { unique: true, name: "pk_episodes" });
db.episodes.createIndex({ parentTconst: 1 }, { name: "fk_ep_parent" });

/* ============================================================
PRINCIPALS ((tconst, ordering) PK)  FK: tconst → title_basics, nconst → name_basics
============================================================ */
db.createCollection("principals", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["tconst", "ordering", "nconst", "category"],
            properties: {
                tconst: { bsonType: "string" },
                ordering: { bsonType: "int" },
                nconst: { bsonType: "string" },
                category: { bsonType: "string" },
                job: { bsonType: ["string", "null"] },
                characters: { bsonType: ["string", "null"] } // fiel a tu SQL (TEXT). Si luego quieres array, lo cambiamos.
            }
        }
    }
});
db.principals.createIndex({ tconst: 1, ordering: 1 }, { unique: true, name: "pk_principals" });
db.principals.createIndex({ tconst: 1 }, { name: "fk_pr_tconst" });
db.principals.createIndex({ nconst: 1 }, { name: "fk_pr_nconst" });

/* ============================================================
RATINGS (tconst PK)  FK: tconst → title_basics
============================================================ */
db.createCollection("ratings", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["tconst", "averageRating", "numVotes"],
            properties: {
                tconst: { bsonType: "string" },
                averageRating: { bsonType: "double" }, // numeric(3,1) → double
                numVotes: { bsonType: "int" }
            }
        }
    }
});
db.ratings.createIndex({ tconst: 1 }, { unique: true, name: "pk_ratings" });

/* ============================================================
NAME PROFESSIONS ((nconst, profession) PK)  FK: nconst → name_basics
============================================================ */
db.createCollection("name_professions", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["nconst", "profession"],
            properties: {
                nconst: { bsonType: "string" },
                profession: { bsonType: "string" }
            }
        }
    }
});
db.name_professions.createIndex({ nconst: 1, profession: 1 }, { unique: true, name: "pk_name_professions" });
db.name_professions.createIndex({ nconst: 1 }, { name: "fk_np_nconst" });

/* ============================================================
NAME KNOWN FOR ((nconst, tconst) PK)  FK: nconst → name_basics, tconst → title_basics
============================================================ */
db.createCollection("name_known_for", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["nconst", "tconst"],
            properties: {
                nconst: { bsonType: "string" },
                tconst: { bsonType: "string" }
            }
        }
    }
});

db.name_known_for.createIndex({ nconst: 1, tconst: 1 }, { unique: true, name: "pk_name_known_for" });
db.name_known_for.createIndex({ nconst: 1 }, { name: "fk_nk_nconst" });
db.name_known_for.createIndex({ tconst: 1 }, { name: "fk_nk_tconst" });

/* ===== FIN ===== */