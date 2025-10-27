# Proyecto Fase 3
## Bases de Datos 2 - Grupo 8

## Justificación del proyecto

La Fase 3 consiste en migrar desde SQL a MongoDB. Diseñamos una arquitectura por capas:

**Capa de colecciones originales (datos de origen):** conservan la normalización del esquema SQL para facilitar la carga masiva (importación masiva y upserts por colección), mantener trazabilidad y poder reconstruir resultados de manera mas rapida.

**Capa de colecciones materializadas (lectura / consulta):** definidas por patrón de acceso, no es 1:1 con tablas. Aquí denormalizamos selectivamente y precomputamos agregados para responder las consultas exigidas con baja latencia.

Los joins de SQL se emulan con `$lookup` en pipelines de agregación. Las llaves primarias/foráneas se simulan con validadores `jsonSchema`, índices únicos y convenciones de identificadores (p. ej., `tconst` en `title_basics` referenciado desde `ratings`, `episodes`, `principals`, etc.).

## Consultas

Las consultas se resolvieron con pipelines y colecciones materializadas:

- **`titles_view`:** construida desde `title_basics` + `ratings` + `basics_genres` + `episodes` + `crew_directors`/`crew_writers` + `name_basics` mediante `aggregate` + `$merge`. Incluye: rating, géneros, conteo de temporadas/episodios y directores/escritores.
Esta coleccion fue pensada de esta manera especifica mas que todo para la realizacion de la primera consulta, donde nosotros elegimos estos campos para que sean mostrados al momento de que ingresar el nombre del titulo, ya luego tambien aprovechamos la inclusion de los directores para mapear las peliculas de este mismo, y finalmente ya que tambien tenemos ratings podemos obtener el top 10 de peliculas facil y rapidamente.

  - Responde a información por nombre de título ademas de que sirve de base para películas de un director por su nombre y Top 10 por rating sin tocar colecciones originales.

- **`material_director_movie_counts`:** conteo de películas por director (origen: `title_basics` + `principals` + `name_basics`).  
Esta colección materializada fue diseñada específicamente para optimizar consultas de agregación sobre directores. Al calcular el conteo total de películas por director, evitamos realizar joins costosos y operaciones de agrupamiento en tiempo real. La estructura simple (director + contador) permite respuestas instantáneas gracias un índice compuesto que ordena por cantidad de películas.

  - Responde a director con más películas.

- **`material_actor_movie_counts`:** conteo de películas por actor/actriz (origen: `title_basics` + `principals` + `name_basics`).  
Similar a la materialización de directores, esta colección calcula estadísticas de actores para evitar agregaciones pesadas en cada consulta. Dado que los actores normalmente participan en más producciones que los directores, materializar estos conteos es crítico para el rendimiento. El diseño permite obtener rankings (top 10, top 100, cosa que como tal en la funcion ya solo pedimos 10 unicamente) sin procesar millones de registros de `principals`, reduciendo drásticamente la velocidad de consulta.

  - Responde Top 10 actores con más películas.

Además, se incluyó un patch específico para series/miniseries que recalcula episodios por temporada antes de fusionar con `titles_view`.

**Regla:** Consultamos solo las materializadas; las colecciones originales fueron usadas como apoyo para el llenado de datos de manera mas simple desde el script de carga masiva a la base, asi dando la oportunidad de trabajar las colecciones materializadas mas rapidamente (ya que ya contamos con los datos en la base y no desde el TSV) y aclarar de nuevo que estas colecciones no son para la lectura.

### Funciones de consulta

Las siguientes funciones leen exclusivamente de colecciones materializadas:

1. **`InfoTitulo(nombre)`** - Consulta desde `titles_view`
   - Busca títulos por nombre exacto (`primaryTitle`)
   - Retorna toda la información del título incluyendo rating, géneros, directores y escritores

2. **`peliculasDeDirector(nombreDirector)`** - Consulta desde `titles_view`
   - Match exacto por nombre de director en array `directors`
   - Filtra solo `titleType: "movie"`
   - Ordena por año de inicio y título

3. **`top10PeliculasMejorRating()`** - Consulta desde `titles_view`
   - Filtra películas con rating numérico
   - Orden por `rating.averageRating` descendente
   - Desempate por `rating.numVotes` descendente
   - Limita a 10 resultados

4. **`directorConMasPeliculas()`** - Consulta desde `material_director_movie_counts`
   - Ordena por cantidad de películas descendente
   - Desempate por nombre de director
   - Retorna el director con más películas

5. **`top10ActoresConMasPeliculas()`** - Consulta desde `material_actor_movie_counts`
   - Ordena por cantidad de películas descendente
   - Desempate por nombre de actor
   - Retorna los 10 actores con más películas

## Pasos para la Fase 3

1. **Validadores y esquema:** se definieron `jsonSchema`, `required` y `bsonType` por colección base; se simularon PK/FK con índices únicos y referencias por campo (p. ej., `tconst`, `nconst`).

2. **Carga masiva:** lectura por lotes de archivos TSV, mapeo a campos MongoDB y upsert colección a colección (análogo a tabla por tabla en SQL), haciendo uso del archivo de carga masiva en Python (modificado de la Fase 1) para conectar e insertar en MongoDB.

3. **Creación de materializadas:** `aggregate` con `$lookup`/`$group` y `$merge` hacia `titles_view`, `material_director_movie_counts` y `material_actor_movie_counts`. Se aplicó el patch de episodios para `tvSeries`/`tvMiniSeries`.

4. **Consultas:** funciones que leen exclusivamente de materializadas (ver sección **Funciones de consulta** arriba).

## Índices (resumen)

### `titles_view`
- `{ tconst: 1 }` unique (idempotencia).
- `{ titleType: 1, "rating.averageRating": -1, "rating.numVotes": -1 }` (Top 10 por rating).
- `{ titleType: 1, directors: 1, startYear: 1, primaryTitle: 1 }` (películas por director con orden estable).

### `material_director_movie_counts`
- `{ peliculas: -1, director: 1 }`.

### `material_actor_movie_counts`
- `{ peliculas: -1, actor: 1 }`.

**Nota:** si se reconstruye una materializada con `$out`, se recrean índices posteriormente.

## Beneficios del proyecto

- **Migración rápida y controlada:** la capa original simplifica la ingesta masiva y preserva la "fuente de verdad".
- **Rendimiento en lectura:** consultas frecuentes salen de materializadas con índices adecuados.
- **Trazabilidad y reconstrucción:** se pueden recalcular vistas ante cambios en datos o reglas sin perder consistencia.
- **Cumplimiento del enunciado:** se diseñan colecciones por uso, no por espejo 1:1 de tablas.

## Desventajas y consideraciones

- Espacio adicional por materialización.