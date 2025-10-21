import os
import csv
import sys
import time  # <-- añadido para heartbeat
from typing import Optional, List, Dict, Any
from pymongo import MongoClient, ReplaceOne
from pymongo.errors import BulkWriteError, ServerSelectionTimeoutError, ConnectionFailure, ConfigurationError

# -----------------------------------------------
# Configuración
# -----------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin@localhost:27017/?authSource=admin")
DB_NAME   = os.getenv("MONGO_DB", "bases2_proyecto")
BASE_DIR  = os.getenv("IMDB_DATA_DIR", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data")))

# LIMIT <= 0 => sin límite (carga COMPLETA)
LIMIT_PER_FILE = int(os.getenv("LIMIT", "-1"))
BATCH = int(os.getenv("BATCH", "35000"))  # sube este valor para más velocidad (p. ej. 20000)

# Heartbeat (progreso) - se pueden ajustar por variables de entorno
PROG_EVERY_ROWS = int(os.getenv("PROG_EVERY_ROWS", "200000"))  # imprime cada N filas leídas
PROG_EVERY_SECS = int(os.getenv("PROG_EVERY_SECS", "60"))      # o cada N segundos

# CSV grandes
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**31 - 1)

# -----------------------------------------------
# Helpers
# -----------------------------------------------
def _none(x: Optional[str]) -> Optional[str]:
    if x is None:
        return None
    x = x.strip()
    return None if x in (r"\N", "") else x

def _to_int(x: Optional[str]) -> Optional[int]:
    x = _none(x)
    if x is None:
        return None
    try:
        return int(x)
    except (ValueError, TypeError):
        return None

def _to_float(x: Optional[str]) -> Optional[float]:
    x = _none(x)
    if x is None:
        return None
    try:
        return float(x)
    except (ValueError, TypeError):
        return None

def _to_bool_01(x: Optional[str]) -> bool:
    return True if _none(x) == "1" else False

def _split_csv(x: Optional[str]) -> List[str]:
    x = _none(x)
    if x is None:
        return []
    return [p.strip() for p in x.split(",") if p.strip()]

def _limit_reached(processed: int) -> bool:
    return LIMIT_PER_FILE > 0 and processed >= LIMIT_PER_FILE

def _bulk_flush(col, ops: List[Any], totals: Dict[str, int]):
    if not ops:
        return
    try:
        res = col.bulk_write(ops, ordered=False, bypass_document_validation=True)
        totals["batches"] += 1
        totals["ops"] += len(ops)
    except BulkWriteError:
        totals["errors"] += 1
    finally:
        ops.clear()

# Verificar conexion correcta a mongo
def mongo_connected(uri: str) -> bool:
    try:
        test_client = MongoClient(
            uri,
            serverSelectionTimeoutMS=5000,  # 5s para fallo rápido
            connectTimeoutMS=5000,
        )
        test_client.admin.command("ping")  # fuerza conexión
        return True
    except (ServerSelectionTimeoutError, ConnectionFailure, ConfigurationError):
        return False

# -----------------------------------------------
# Cargas (todas con UPSERT idempotente)
# -----------------------------------------------
def load_title_basics(db):
    path = os.path.join(BASE_DIR, "title.basics.tsv")
    col = db["title_basics"]
    col.create_index("tconst", unique=True)

    ops, processed = [], 0
    totals = {"ops": 0, "batches": 0, "errors": 0}

    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            tconst = row["tconst"]
            doc = {
                "tconst": tconst,
                "titleType": _none(row["titleType"]) or r"\N",
                "primaryTitle": _none(row["primaryTitle"]) or r"\N",
                "originalTitle": _none(row["originalTitle"]) or r"\N",
                "isAdult": _to_bool_01(row["isAdult"]),
                "startYear": _to_int(row["startYear"]),
                "endYear": _to_int(row["endYear"]),
                "runtimeMinutes": _to_int(row["runtimeMinutes"]),
            }
            ops.append(ReplaceOne({"tconst": tconst}, doc, upsert=True))
            processed += 1
            if len(ops) >= BATCH: _bulk_flush(col, ops, totals)
            if _limit_reached(processed): break
    _bulk_flush(col, ops, totals)
    print(f"[OK] title_basics: {processed} filas procesadas | col.count={col.count_documents({})} | batches={totals['batches']} errors={totals['errors']}")
    return set(doc["tconst"] for doc in col.find({}, {"_id":0,"tconst":1}))

def load_basics_genres(db):
    path = os.path.join(BASE_DIR, "title.basics.tsv")
    col = db["basics_genres"]
    col.create_index([("tconst", 1), ("genre", 1)], unique=True)
    col.create_index("tconst")

    ops, processed = [], 0
    totals = {"ops": 0, "batches": 0, "errors": 0}

    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            tconst = row["tconst"]
            primaryTitle = _none(row["primaryTitle"]) or r"\N"
            for g in _split_csv(row.get("genres")):
                key = {"tconst": tconst, "genre": g}
                doc = {"tconst": tconst, "primaryTitle": primaryTitle, "genre": g}
                ops.append(ReplaceOne(key, doc, upsert=True))
                if len(ops) >= BATCH: _bulk_flush(col, ops, totals)
            processed += 1
            if _limit_reached(processed): break
    _bulk_flush(col, ops, totals)
    print(f"[OK] basics_genres: {processed} filas procesadas | col.count={col.count_documents({})} | batches={totals['batches']} errors={totals['errors']}")

def load_akas(db):
    path = os.path.join(BASE_DIR, "title.akas.tsv")
    akas = db["akas"];              akas.create_index([("titleId",1),("ordering",1)], unique=True)
    aka_types = db["aka_types"];    aka_types.create_index([("titleId",1),("ordering",1),("type",1)], unique=True)
    aka_attrs = db["aka_attributes"]; aka_attrs.create_index([("titleId",1),("ordering",1),("attribute",1)], unique=True)
    akas.create_index("titleId"); aka_types.create_index([("titleId",1),("ordering",1)]); aka_attrs.create_index([("titleId",1),("ordering",1)])

    ops_aka, ops_typ, ops_att, processed = [], [], [], 0
    totalsA = {"ops":0,"batches":0,"errors":0}
    totalsT = {"ops":0,"batches":0,"errors":0}
    totalsAt = {"ops":0,"batches":0,"errors":0}

    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            titleId = row["titleId"]
            ordering = _to_int(row["ordering"]) or 0
            doc_aka = {
                "titleId": titleId,
                "ordering": ordering,
                "title": _none(row["title"]) or r"\N",
                "region": _none(row.get("region")),
                "isOriginalTitle": _to_bool_01(row.get("isOriginalTitle")),
            }
            ops_aka.append(ReplaceOne({"titleId": titleId, "ordering": ordering}, doc_aka, upsert=True))

            for t in _split_csv(row.get("types")):
                ops_typ.append(ReplaceOne({"titleId": titleId, "ordering": ordering, "type": t},
                                        {"titleId": titleId, "ordering": ordering, "type": t}, upsert=True))
                if len(ops_typ) >= BATCH: _bulk_flush(aka_types, ops_typ, totalsT)
            for a in _split_csv(row.get("attributes")):
                ops_att.append(ReplaceOne({"titleId": titleId, "ordering": ordering, "attribute": a},
                                        {"titleId": titleId, "ordering": ordering, "attribute": a}, upsert=True))
                if len(ops_att) >= BATCH: _bulk_flush(aka_attrs, ops_att, totalsAt)

            if len(ops_aka) >= BATCH: _bulk_flush(akas, ops_aka, totalsA)
            processed += 1
            if _limit_reached(processed): break

    _bulk_flush(akas, ops_aka, totalsA)
    _bulk_flush(aka_types, ops_typ, totalsT)
    _bulk_flush(aka_attrs, ops_att, totalsAt)

    print(f"[OK] akas: rows={processed} | col.count={akas.count_documents({})} | batches={totalsA['batches']} errors={totalsA['errors']}")
    print(f"[OK] aka_types: col.count={aka_types.count_documents({})} | batches={totalsT['batches']} errors={totalsT['errors']}")
    print(f"[OK] aka_attributes: col.count={aka_attrs.count_documents({})} | batches={totalsAt['batches']} errors={totalsAt['errors']}")

def load_name_basics(db):
    path = os.path.join(BASE_DIR, "name.basics.tsv")
    col = db["name_basics"]
    col.create_index("nconst", unique=True)

    ops, processed = [], 0
    totals = {"ops":0,"batches":0,"errors":0}

    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            nconst = row["nconst"]
            doc = {
                "nconst": nconst,
                "primaryName": _none(row["primaryName"]) or r"\N",
                "birthYear": _to_int(row.get("birthYear")),
                "deathYear": _to_int(row.get("deathYear")),
            }
            ops.append(ReplaceOne({"nconst": nconst}, doc, upsert=True))
            processed += 1
            if len(ops) >= BATCH: _bulk_flush(col, ops, totals)
            if _limit_reached(processed): break

    _bulk_flush(col, ops, totals)
    print(f"[OK] name_basics: {processed} filas procesadas | col.count={col.count_documents({})} | batches={totals['batches']} errors={totals['errors']}")
    return set(doc["nconst"] for doc in col.find({}, {"_id":0,"nconst":1}))

def load_name_professions(db):
    path = os.path.join(BASE_DIR, "name.basics.tsv")
    col = db["name_professions"]
    col.create_index([("nconst",1),("profession",1)], unique=True)
    col.create_index("nconst")

    ops, processed = [], 0
    totals = {"ops":0,"batches":0,"errors":0}

    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            nconst = row["nconst"]
            for prof in _split_csv(row.get("primaryProfession")):
                key = {"nconst": nconst, "profession": prof}
                ops.append(ReplaceOne(key, key, upsert=True))
                if len(ops) >= BATCH: _bulk_flush(col, ops, totals)
            processed += 1
            if _limit_reached(processed): break

    _bulk_flush(col, ops, totals)
    print(f"[OK] name_professions: {processed} filas procesadas | col.count={col.count_documents({})} | batches={totals['batches']} errors={totals['errors']}")

def load_name_known_for(db):
    path = os.path.join(BASE_DIR, "name.basics.tsv")
    col = db["name_known_for"]
    col.create_index([("nconst",1),("tconst",1)], unique=True)
    col.create_index("nconst"); col.create_index("tconst")

    ops, processed = [], 0
    totals = {"ops":0,"batches":0,"errors":0}

    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            nconst = row["nconst"]
            for tconst in _split_csv(row.get("knownForTitles")):
                key = {"nconst": nconst, "tconst": tconst}
                ops.append(ReplaceOne(key, key, upsert=True))
                if len(ops) >= BATCH: _bulk_flush(col, ops, totals)
            processed += 1
            if _limit_reached(processed): break

    _bulk_flush(col, ops, totals)
    print(f"[OK] name_known_for: {processed} filas procesadas | col.count={col.count_documents({})} | batches={totals['batches']} errors={totals['errors']}")

def load_crew(db):
    path = os.path.join(BASE_DIR, "title.crew.tsv")
    cd = db["crew_directors"]; cd.create_index([("tconst",1),("nconst",1)], unique=True)
    cw = db["crew_writers"];  cw.create_index([("tconst",1),("nconst",1)], unique=True)
    cd.create_index("tconst"); cd.create_index("nconst")
    cw.create_index("tconst"); cw.create_index("nconst")

    ops_cd, ops_cw, processed = [], [], 0
    totalsD = {"ops":0,"batches":0,"errors":0}
    totalsW = {"ops":0,"batches":0,"errors":0}

    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            tconst = row["tconst"]
            for d in _split_csv(row.get("directors")):
                key = {"tconst": tconst, "nconst": d}
                ops_cd.append(ReplaceOne(key, key, upsert=True))
                if len(ops_cd) >= BATCH: _bulk_flush(cd, ops_cd, totalsD)
            for w in _split_csv(row.get("writers")):
                key = {"tconst": tconst, "nconst": w}
                ops_cw.append(ReplaceOne(key, key, upsert=True))
                if len(ops_cw) >= BATCH: _bulk_flush(cw, ops_cw, totalsW)

            processed += 1
            if _limit_reached(processed): break

    _bulk_flush(cd, ops_cd, totalsD)
    _bulk_flush(cw, ops_cw, totalsW)
    print(f"[OK] crew_directors: col.count={cd.count_documents({})} | batches={totalsD['batches']} errors={totalsD['errors']}")
    print(f"[OK] crew_writers:  col.count={cw.count_documents({})} | batches={totalsW['batches']} errors={totalsW['errors']}")

def load_episodes(db):
    path = os.path.join(BASE_DIR, "title.episode.tsv")
    col = db["episodes"]
    col.create_index("tconst", unique=True)
    col.create_index("parentTconst")

    ops, processed = [], 0
    totals = {"ops":0,"batches":0,"errors":0}

    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            tconst = row["tconst"]
            doc = {
                "tconst": tconst,
                "parentTconst": _none(row.get("parentTconst")),
                "seasonNumber": _to_int(row.get("seasonNumber")),
                "episodeNumber": _to_int(row.get("episodeNumber"))
            }
            ops.append(ReplaceOne({"tconst": tconst}, doc, upsert=True))
            processed += 1
            if len(ops) >= BATCH: _bulk_flush(col, ops, totals)
            if _limit_reached(processed): break

    _bulk_flush(col, ops, totals)
    print(f"[OK] episodes: {processed} filas procesadas | col.count={col.count_documents({})} | batches={totals['batches']} errors={totals['errors']}")

def load_principals(db):
    path = os.path.join(BASE_DIR, "title.principals.tsv")
    col = db["principals"]
    col.create_index([("tconst",1),("ordering",1)], unique=True)
    col.create_index("tconst"); col.create_index("nconst")

    ops, processed = [], 0
    totals = {"ops":0,"batches":0,"errors":0}

    # ------ Heartbeat ------
    t0 = time.perf_counter()
    last_print = t0
    print("[INICIANDO] principals")
    # -----------------------

    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            key = {"tconst": row["tconst"], "ordering": _to_int(row["ordering"]) or 0}
            doc = {
                "tconst": row["tconst"],
                "ordering": _to_int(row["ordering"]) or 0,
                "nconst": row["nconst"],
                "category": _none(row.get("category")) or r"\N",
                "job": _none(row.get("job")),
                "characters": _none(row.get("characters")),
            }
            ops.append(ReplaceOne(key, doc, upsert=True))
            processed += 1

            if len(ops) >= BATCH:
                _bulk_flush(col, ops, totals)

            # ------ Heartbeat por filas o por tiempo ------
            now = time.perf_counter()
            if (processed % PROG_EVERY_ROWS == 0) or (now - last_print >= PROG_EVERY_SECS):
                elapsed = (now - t0) / 60.0
                print(f"[HB principals] processed={processed:,} | batches={totals['batches']:,} "
                      f"| ops={totals['ops']:,} | errors={totals['errors']} | elapsed={elapsed:,.2f} min")
                last_print = now
            # ----------------------------------------------

            if _limit_reached(processed): break

    _bulk_flush(col, ops, totals)
    print(f"[OK] principals: {processed} filas procesadas | col.count={col.count_documents({})} | batches={totals['batches']} errors={totals['errors']}")

def load_ratings(db):
    path = os.path.join(BASE_DIR, "title.ratings.tsv")
    col = db["ratings"]
    col.create_index("tconst", unique=True)
    col.create_index("numVotes")

    ops, processed = [], 0
    totals = {"ops":0,"batches":0,"errors":0}

    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            tconst = row["tconst"]
            doc = {
                "tconst": tconst,
                "averageRating": _to_float(row["averageRating"]) or 0.0,
                "numVotes": _to_int(row["numVotes"]) or 0
            }
            ops.append(ReplaceOne({"tconst": tconst}, doc, upsert=True))
            processed += 1
            if len(ops) >= BATCH: _bulk_flush(col, ops, totals)
            if _limit_reached(processed): break

    _bulk_flush(col, ops, totals)
    print(f"[OK] ratings: {processed} filas procesadas | col.count={col.count_documents({})} | batches={totals['batches']} errors={totals['errors']}")

# -----------------------------------------------
# Main
# -----------------------------------------------
if __name__ == "__main__":

    print(f"Conectando a Mongo en: {MONGO_URI}")
    if not mongo_connected(MONGO_URI):
        print("[FALLO] No se pudo conectar (ping falló). Revisa contenedor/credenciales/puerto.")
        sys.exit(1)

    print("Conectando a Mongo…")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    print("[OK] Conectado. Base seleccionada:", DB_NAME)

    # # 1) Padres
    # print("Iniciando title_basics")
    # title_keys = load_title_basics(db)

    # print("Iniciando basics_genres")
    # load_basics_genres(db)

    # print("Iniciando akas")
    # load_akas(db)

    # print("Iniciando name_basics")
    # name_keys = load_name_basics(db)

    # print("Iniciando name_professiones")
    # load_name_professions(db)

    # print("Iniciando name_know_for")
    # load_name_known_for(db)

    # # 2) Relaciones
    # print("Iniciando crew")
    # load_crew(db)

    # print("Iniciando episodes")
    # load_episodes(db)

    print("Iniciando principals")
    load_principals(db)

    print("Iniciando ratings")
    load_ratings(db)

    print("Carga finalizada.")
