# Proyecto Fase 3 
# Bases de datos 2

## Grupo 8

### Justificación del proyecto
La fase 3 del proyecto se basa en la migración de una base de datos desde SQL  MongoDB, para esto el grupo 8 mantuvo la normalización que se habia utilizado en la base de datos SQL esto debido a que se queria priorizar la lógica que se traía en esa base de datos y con eso facilitar la migración a la tecnología MongoDB debido a que con esto se minimizan los cambios tambien y se ayuda a que las consultas tengan relaciones mas explicitas. 
Al utilizar la normalización tambien ayudamos a la migración debido a que os Joins de SQL pueden asimilarse a los lookuo de Mongo, tambien la carga masiva se ve beneficiada ya que se puede utilizar un upsert haciendo colección por colección asi como tabla por tabal en SQL  al igual que con los lookups se pueden utilizar para emular JOINS y asi usarlos para las consultas. 

Se simularon las llaves primarias y llaves foráneas con colecciones y jsonSchemas. Esto como por ejemplo con title_basics simulamos el tconst como primary key con ratings episodes y principal referenciando a tconst y asi simulando una llave primaria.

### Consultas
Las consultas se manejaron con pipelines y lookups, utilizando por ejemplo para la información de un título en concreto una vista llamada titles_view esta vista utiliza gruop y merge en esa colección para calcular las temporadas, los episodios y los episodios por temporada. 
Se manejaron colecciones materializadas con las consultas para tener patrones que emulen consultas relacionales. 

### Pasos para la fase 3
* Para forzar la estructura predefinida en SQL se utilizo para las tablas se usa colección con jsonSchema, required y bsontype y se simulan llaves primarias como se muestra en la parte de consultas. 
* Se crean los índices para las busquedas e indices cubrientes lo que nos asegura un buen tiempo en lookups y sorts, por útltimo indices compuestos para prevenir duplicaciones. 
* Se crean vistas materializadas para las consultas, para esto se usa el agregate y merge tambien se incluye patch para las series y miniseries y que esto recalcule los episodios. 
* En el archivo de funcniones se hacen las consultas tanto directas como a titles_view como se ordenan tambien por rating y vosots para resolver las consultas tal cual se pidieron en la fase1.
* La carga masiva a la tecnología MongoDB sehace leyendo los archivos tsv por lotes y se mapean los campos de Mongo. Para cada colección creada se respeto el orden que se tenía en SQL por las llaves primarias que fueron simuladas dentro de la base.

### Bneficios del proyecto 
Se hace una migración rapida desde SQL a MongoDB aparte de que matiene una integridad y consistencia de la base de datos, también le da una reutilización a la base para las personas que utilizan la base debido que es facil de entender el funcnionamiento por el parecido. 

### Desventajas de la migración 
Algunas lecturas podran ser mas costosas por el diseño de la base. 
La adaptación hace que la creación de la base parezca mas complicada. 

