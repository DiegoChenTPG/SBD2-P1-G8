import os
import csv
import mysql.connector

DB_CONFIG = {  # Esta variable puede tener cualquier nombre
    "host": "192.168.0.8",
    # "host": "localhost",
    "user": "root",                 # Cambia según tu usuario de MySQL
    "password": "bases2_proyecto",  # Cambia según tu contraseña de MySQL
    "database": "base_bases2_proyecto"  # Cambia por el nombre real de tu base de datos
}

#BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data")) # Se navega hasta la carpeta por la estructura que tenemos 

def health_check():
    conn = mysql.connector.connect(**DB_CONFIG)
    try:
        return "Conectada" if conn.is_connected() else "No Conectada"
    finally:
        conn.close()

# ----------------------- Helpers -----------------------

def _none(x: str):
    return None if x is None or x == r"\N" or x == "" else x

def _to_int(x: str):
    x = _none(x);  return None if x is None else int(x)

def _to_float(x: str):
    x = _none(x);  return None if x is None else float(x)

def _to_year(x: str):
    # YEAR en MySQL: acepta 1901–2155 y 0000; si viene fuera de rango, puedes devolver None
    x = _none(x)
    if x is None:
        return None
    try:
        y = int(x)
        # Ajusta si quieres validar rango estrictamente:
        # if y < 0 or y > 2155: return None
        return y
    except:
        return None

def _split_csv(x: str):
    x = _none(x)
    if x is None:
        return []
    return [p.strip() for p in x.split(",") if p.strip()]

def _batch_exec(cursor, sql, batch, commit=False, connection=None):
    if not batch:
        return
    cursor.executemany(sql, batch)
    batch.clear()
    if commit and connection:
        connection.commit()

# ----------------------- Loaders -----------------------

def _load_title_basics_and_genres(cursor, connection):
    path = os.path.join(BASE_DIR, "title.basics.tsv")
    sql_tb = ("INSERT IGNORE INTO title_basics "
              "(tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes) "
              "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)")
    sql_bg = ("INSERT IGNORE INTO basics_genres (tconst, primaryTitle, genre) "
              "VALUES (%s,%s,%s)")
    batch_tb, batch_bg = [], []
    inserted_tb = inserted_bg = 0

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            tconst = row["tconst"]
            titleType = _none(row["titleType"])
            primaryTitle = _none(row["primaryTitle"])
            originalTitle = _none(row["originalTitle"])
            isAdult = 1 if _none(row["isAdult"]) == "1" else 0
            startYear = _to_year(row["startYear"])
            endYear = _to_year(row["endYear"])
            runtimeMinutes = _to_int(row["runtimeMinutes"])

            batch_tb.append((tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes))
            if len(batch_tb) >= 2000:
                cursor.executemany(sql_tb, batch_tb)
                inserted_tb += cursor.rowcount
                batch_tb.clear()
                connection.commit()

            # géneros normalizados
            for g in _split_csv(row.get("genres", None)):
                batch_bg.append((tconst, primaryTitle, g))
                if len(batch_bg) >= 4000:
                    cursor.executemany(sql_bg, batch_bg)
                    inserted_bg += cursor.rowcount
                    batch_bg.clear()
                    connection.commit()

    if batch_tb:
        cursor.executemany(sql_tb, batch_tb)
        inserted_tb += cursor.rowcount
        connection.commit()
    if batch_bg:
        cursor.executemany(sql_bg, batch_bg)
        inserted_bg += cursor.rowcount
        connection.commit()

    return inserted_tb, inserted_bg

def _load_name_basics_professions_known(cursor, connection):
    path = os.path.join(BASE_DIR, "name.basics.tsv")
    sql_nb = ("INSERT IGNORE INTO name_basics (nconst, primaryName, birthYear, deathYear) "
              "VALUES (%s,%s,%s,%s)")
    sql_np = ("INSERT IGNORE INTO name_professions (nconst, profession) "
              "VALUES (%s,%s)")
    sql_nk = ("INSERT IGNORE INTO name_known_for (nconst, tconst) "
              "VALUES (%s,%s)")

    batch_nb, batch_np, batch_nk = [], [], []
    ins_nb = ins_np = ins_nk = 0

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            nconst = row["nconst"]
            primaryName = _none(row["primaryName"])
            birthYear = _to_year(row["birthYear"])
            deathYear = _to_year(row["deathYear"])

            batch_nb.append((nconst, primaryName, birthYear, deathYear))
            if len(batch_nb) >= 2000:
                cursor.executemany(sql_nb, batch_nb)
                ins_nb += cursor.rowcount
                batch_nb.clear()
                connection.commit()

            for prof in _split_csv(row.get("primaryProfession", None)):
                batch_np.append((nconst, prof))
                if len(batch_np) >= 4000:
                    cursor.executemany(sql_np, batch_np)
                    ins_np += cursor.rowcount
                    batch_np.clear()
                    connection.commit()

            for tconst in _split_csv(row.get("knownForTitles", None)):
                batch_nk.append((nconst, tconst))
                if len(batch_nk) >= 4000:
                    cursor.executemany(sql_nk, batch_nk)
                    ins_nk += cursor.rowcount
                    batch_nk.clear()
                    connection.commit()

    if batch_nb:
        cursor.executemany(sql_nb, batch_nb); ins_nb += cursor.rowcount; connection.commit()
    if batch_np:
        cursor.executemany(sql_np, batch_np); ins_np += cursor.rowcount; connection.commit()
    if batch_nk:
        cursor.executemany(sql_nk, batch_nk); ins_nk += cursor.rowcount; connection.commit()

    return ins_nb, ins_np, ins_nk

def _load_title_akas_and_parts(cursor, connection):
    path = os.path.join(BASE_DIR, "title.akas.tsv")
    sql_aka = ("INSERT IGNORE INTO akas "
               "(titleId, ordering, title, region, isOriginalTitle) "
               "VALUES (%s,%s,%s,%s,%s)")
    sql_typ = ("INSERT IGNORE INTO aka_types (titleId, ordering, type) "
               "VALUES (%s,%s,%s)")
    sql_att = ("INSERT IGNORE INTO aka_attributes (titleId, ordering, attribute) "
               "VALUES (%s,%s,%s)")

    b_aka, b_typ, b_att = [], [], []
    ins_aka = ins_typ = ins_att = 0

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            titleId = row["titleId"]
            ordering = _to_int(row["ordering"])
            title = _none(row["title"])
            region = _none(row.get("region", None))
            isOriginalTitle = 1 if _none(row.get("isOriginalTitle", None)) == "1" else 0

            b_aka.append((titleId, ordering, title, region, isOriginalTitle))
            if len(b_aka) >= 2000:
                cursor.executemany(sql_aka, b_aka)
                ins_aka += cursor.rowcount
                b_aka.clear()
                connection.commit()

            for t in _split_csv(row.get("types", None)):
                b_typ.append((titleId, ordering, t))
                if len(b_typ) >= 4000:
                    cursor.executemany(sql_typ, b_typ)
                    ins_typ += cursor.rowcount
                    b_typ.clear()
                    connection.commit()

            for a in _split_csv(row.get("attributes", None)):
                b_att.append((titleId, ordering, a))
                if len(b_att) >= 4000:
                    cursor.executemany(sql_att, b_att)
                    ins_att += cursor.rowcount
                    b_att.clear()
                    connection.commit()

    if b_aka:
        cursor.executemany(sql_aka, b_aka); ins_aka += cursor.rowcount; connection.commit()
    if b_typ:
        cursor.executemany(sql_typ, b_typ); ins_typ += cursor.rowcount; connection.commit()
    if b_att:
        cursor.executemany(sql_att, b_att); ins_att += cursor.rowcount; connection.commit()

    return ins_aka, ins_typ, ins_att

def _load_title_crew(cursor, connection):
    path = os.path.join(BASE_DIR, "title.crew.tsv")
    sql_dir = "INSERT IGNORE INTO crew_directors (tconst, nconst) VALUES (%s,%s)"
    sql_wri = "INSERT IGNORE INTO crew_writers   (tconst, nconst) VALUES (%s,%s)"

    b_dir, b_wri = [], []
    ins_dir = ins_wri = 0

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            tconst = row["tconst"]
            for d in _split_csv(row.get("directors", None)):
                b_dir.append((tconst, d))
                if len(b_dir) >= 4000:
                    cursor.executemany(sql_dir, b_dir); ins_dir += cursor.rowcount; b_dir.clear(); connection.commit()
            for w in _split_csv(row.get("writers", None)):
                b_wri.append((tconst, w))
                if len(b_wri) >= 4000:
                    cursor.executemany(sql_wri, b_wri); ins_wri += cursor.rowcount; b_wri.clear(); connection.commit()

    if b_dir:
        cursor.executemany(sql_dir, b_dir); ins_dir += cursor.rowcount; connection.commit()
    if b_wri:
        cursor.executemany(sql_wri, b_wri); ins_wri += cursor.rowcount; connection.commit()

    return ins_dir, ins_wri

def _load_title_episode(cursor, connection):
    path = os.path.join(BASE_DIR, "title.episode.tsv")
    sql_ep = ("INSERT IGNORE INTO episodes "
              "(tconst, parentTconst, seasonNumber, episodeNumber) "
              "VALUES (%s,%s,%s,%s)")
    b_ep = []; ins_ep = 0
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            tconst = row["tconst"]
            parent = _none(row["parentTconst"])
            season = _to_int(row.get("seasonNumber", None))
            epis   = _to_int(row.get("episodeNumber", None))
            b_ep.append((tconst, parent, season, epis))
            if len(b_ep) >= 2000:
                cursor.executemany(sql_ep, b_ep); ins_ep += cursor.rowcount; b_ep.clear(); connection.commit()
    if b_ep:
        cursor.executemany(sql_ep, b_ep); ins_ep += cursor.rowcount; connection.commit()
    return ins_ep

def _load_title_principals(cursor, connection):
    path = os.path.join(BASE_DIR, "title.principals.tsv")
    sql_pr = ("INSERT IGNORE INTO principals "
              "(tconst, ordering, nconst, category, job, characters) "
              "VALUES (%s,%s,%s,%s,%s,%s)")
    b_pr = []; ins_pr = 0
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            b_pr.append((
                row["tconst"],
                _to_int(row["ordering"]),
                row["nconst"],
                _none(row["category"]),
                _none(row.get("job", None)),
                _none(row.get("characters", None))
            ))
            if len(b_pr) >= 2000:
                cursor.executemany(sql_pr, b_pr); ins_pr += cursor.rowcount; b_pr.clear(); connection.commit()
    if b_pr:
        cursor.executemany(sql_pr, b_pr); ins_pr += cursor.rowcount; connection.commit()
    return ins_pr

def _load_title_ratings(cursor, connection):
    path = os.path.join(BASE_DIR, "title.ratings.tsv")
    sql_rt = ("INSERT IGNORE INTO ratings "
              "(tconst, averageRating, numVotes) "
              "VALUES (%s,%s,%s)")
    b_rt = []; ins_rt = 0
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            b_rt.append((row["tconst"], _to_float(row["averageRating"]), _to_int(row["numVotes"])))
            if len(b_rt) >= 4000:
                cursor.executemany(sql_rt, b_rt); ins_rt += cursor.rowcount; b_rt.clear(); connection.commit()
    if b_rt:
        cursor.executemany(sql_rt, b_rt); ins_rt += cursor.rowcount; connection.commit()
    return ins_rt

# ----------------------- Orquestador -----------------------

def carga_masiva():
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        connection.autocommit = False
        cursor = connection.cursor()

        # 1) Títulos (y géneros normalizados)
        ins_tb, ins_bg = _load_title_basics_and_genres(cursor, connection)

        # 2) Personas (y normalizaciones)
        ins_nb, ins_np, ins_nk = _load_name_basics_professions_known(cursor, connection)

        # 3) AKAS y sus types/attributes
        ins_aka, ins_typ, ins_att = _load_title_akas_and_parts(cursor, connection)

        # 4) Crew (directors, writers)
        ins_dir, ins_wri = _load_title_crew(cursor, connection)

        # 5) Episodes
        ins_ep = _load_title_episode(cursor, connection)

        # 6) Principals
        ins_pr = _load_title_principals(cursor, connection)

        # 7) Ratings
        ins_rt = _load_title_ratings(cursor, connection)

        connection.commit()

        resumen = (
            f"title_basics: {ins_tb}, basics_genres: {ins_bg}, "
            f"name_basics: {ins_nb}, name_professions: {ins_np}, name_known_for: {ins_nk}, "
            f"akas: {ins_aka}, aka_types: {ins_typ}, aka_attributes: {ins_att}, "
            f"crew_directors: {ins_dir}, crew_writers: {ins_wri}, "
            f"episodes: {ins_ep}, principals: {ins_pr}, ratings: {ins_rt}"
        )
        print("Resumen inserts (pueden ser 0 si ya existían por INSERT IGNORE):")
        print(resumen)
        return "datos cargados correctamente"
    except Exception as e:
        if connection:
            connection.rollback()
        print(f"Error al procesar los datos de entrada: {e}")
        return "Error al procesar los datos de entrada", str(e)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    print(health_check())
    # print(carga_masiva())
