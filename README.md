# ProyectoIntegrador_PFR
Repositorio del proyecto integrador - Programación funcional y reactica
### Tabla de datos
| Nombre columna | Tipo de Dato | Proposito | Observacones |
|----------------|--------------|-----------|--------------|
| adult | boolean | Comprueba si la pelicula es para mayores de edad | No deberia ser null |
| belongs_to_collection | String | Indica si la pelicula pertenece a una coleccion especifica | Opcional algunos valores pueden ser nulos |
| budget | Int | Indica el costo de produccion de la pelicula | Opcional algunos valores pueden ser nulos |
| genres | String | Indica la lista de generos de la pelicula | Necesaria para brindar informacion adicional de la pelicula |
| homepage | String | Contiene el URL del sitio ofcial de la pelicula | Necesaria para brindar informacion adicional de la pelicula |
| id | Int | Identificador unico asignado a cada pelicula | No se repite en todo el dataset |
| imdb_id | String | Identificador unico asignado por IMDB a cada pelicula | No se repite en todo el dataset |
| original_lenguage | String | Indica el idioma original de la pelicula | No deberia ser null |
| original_title | String | Indica el titulo de la pelicula en su idioma original | No deberia ser null |
| overview | String | Contiene una breve descripción o sinopsis de la trama de la película | Necesaria para brindar informacion adicional de la pelicula |
| popularity | Double | Mide la popularidad de la pelicula | Opcional algunos valores pueden ser nulos |
| poster_path | String | Contiene la URL de una imagen del poster de la pelicula | Necesaria para brindar informacion adicional de la pelicula |
| production_companies | String | Contiene un JSON con la lista de las empresas de producción involucradas en la creación de la película | Necesaria para brindar informacion adicional de la pelicula |
| production_countries | String | Contiene un JSON que lista los países en los cuales se grabó la película | Necesaria para brindar informacion adicional de la pelicula |
|	release_date | String | Contiene la fecha en que la película fue lanzada por primera vez en los cines | Formato: YYYY-MM-DD |
|	revenue | Long | Contiene los ingresos totales generados por la pelicula | No deberia ser null |
| runtime | Int | Duracion de la pelicula | Necesaria para brindar informacion adicional de la pelicula |
| spoken_languages | String | Representa los idiomas en los que se habla durante la película | Necesaria para brindar informacion adicional de la pelicula |
| status | String | Indica el estado de la pelicula en producion, completa, etc... | Opcional algunos valores pueden ser nulos |
| tagline | String | Frase breve para promocionar la pelicula | Opcional algunos valores pueden ser nulos |
| title | String | Contiene el titulo oficial de la pelicula | No deberia ser null |
| video | Boolean | Indica si la pelicula tiene una version en video | No deberia ser null |
| vote_average | Double | Indica el promedio de la calificación que los usuarios han dado a la película | Opcional algunos valores pueden ser nulos |
| vote_count | Int | Indica el numero total de votos o calificaciones que ha recibido la película | Opcional algunos valores pueden ser nulos |
| keywords | String | Contiene un JSON con una lista de palabras clave asociadas a la película | No deberia ser null |
| cast | String | Contiene los datos de los actores y actrices que participaron en la película | No deberia ser null |
| crew | String | Contiene información sobre el equipo de producción de la película | No deberia ser null |
| ratings | String | Contiene las calificaciones de la película de diferentes fuentes | Opcional algunos valores pueden ser nulos |

### Descripción de Columnas
•	adult (Boolean):
Esta columna indica si la película es para mayores de edad. Se usa para determinar si el contenido tiene restricciones de edad debido a su violencia, lenguaje, o contenido sexual explícito.

•	belongs_to_collection (String):
Esta columna indica si la película pertenece a una colección específica, como una saga de películas o una serie de franquicias. El valor es una cadena de texto que contiene el nombre de la colección o el identificador único de la misma.

•	budget (Int):
Esta columna indica el costo de producir la película, incluyendo el pago de los actores, el equipo técnico, efectos especiales, marketing, etc. El valor es un número entero que representa el presupuesto total de la película en dólares.

•	genres (Lo manejaremos como String pero es JSON):
Esta columna contiene una lista de géneros, categorías o temas principales de la película, como acción, drama, comedia, etc. El valor está representado como una cadena de texto que contiene un JSON con los géneros de la película.

•	homepage (String):
Esta columna contiene la URL del sitio web oficial de la película. Generalmente es utilizada para obtener información adicional, como trailers, sinopsis, y otros datos promocionales.

•	id (Int):
Esta columna representa un identificador único asignado a cada película dentro del dataset. Sirve para referenciar de manera unívoca a cada película en la base de datos.

•	imdb_id (String):
Esta columna representa el identificador único asignado por IMDb (Internet Movie Database) a cada película. Este ID permite enlazar fácilmente cada película con su página en IMDb.

•	original_language (String):
Esta columna indica el idioma original en el que se realizó y publicó la película, por ejemplo, inglés, español, francés, etc.

•	original_title (String):
Esta columna contiene el título de la película en su idioma original, tal como fue lanzada al público en el país de origen.

•	overview (String):
Esta columna contiene una breve descripción o sinopsis de la trama de la película, con el propósito de dar al lector una visión general de la historia que se presenta.

•	popularity (Double):
Esta columna es un valor numérico que mide la popularidad de la película. Se calcula basándose en varios factores como vistas, menciones en redes sociales, y más.

•	poster_path (String):
Esta columna contiene la URL o el camino relativo hacia la imagen del póster de la película, que es utilizada en diversos medios promocionales.

•	production_companies (Lo manejaremos como String pero es JSON):
Esta columna contiene un JSON con la lista de las empresas de producción involucradas en la creación de la película. Estas compañías financian, producen y distribuyen la película.

•	production_countries (Lo manejaremos como String pero es JSON):
Esta columna contiene un JSON que lista los países en los cuales se grabó la película. Cada película debe tener, como mínimo, un país de producción.

•	release_date (String):
Esta columna es de tipo fecha (string), y contiene la fecha en que la película fue lanzada por primera vez en los cines. Generalmente se usa el formato YYYY-MM-DD.

•	revenue (Long):
Esta columna representa los ingresos totales generados por la película a través de taquilla, ventas de DVD/Blu-ray, y otros ingresos comerciales. El valor es un número largo (long) que refleja las ganancias totales.

•	runtime (Int):
Esta columna indica la duración total de la película, medida en minutos. Representa el tiempo en pantalla de la película desde el inicio hasta el final.

•	spoken_languages (Lo manejaremos como String pero es JSON):
Esta columna es de tipo JSON y representa los idiomas en los que se habla durante la película. Cada idioma es listado en formato JSON, por ejemplo, español, inglés, francés, etc.

•	status (String):
Esta columna indica el estado de la película, como si está en producción, completa, anunciada, cancelada, etc. Proporciona información sobre la etapa en la que se encuentra la película.

•	tagline (String):
Esta columna contiene una frase breve que generalmente se utiliza para promocionar la película, como un eslogan o lema que resalta su temática o estilo.

•	title (String):
Esta columna contiene el título oficial con el cual la película es presentada al público en general. Es el nombre con el que la película será reconocida en medios.

•	video (Boolean):
Esta columna es de tipo booleano y se usa para indicar si la película tiene una versión en video, como un trailer o un lanzamiento en formato de video.

•	vote_average (Double):
Esta columna representa el promedio de la calificación que los usuarios han dado a la película, en plataformas como IMDb, Rotten Tomatoes o Metacritic.

•	vote_count (Int):
Esta columna muestra el número total de votos o calificaciones que ha recibido la película. Es un indicador de la popularidad y aceptación de la película entre el público.

•	keywords (Lo manejaremos como String pero es JSON):
Esta columna contiene un JSON con una lista de palabras clave asociadas a la película. Estas palabras clave son términos que representan los temas, géneros, conceptos o elementos importantes relacionados con la película.

•	cast (Lo manejaremos como String pero es JSON):
Esta columna es de tipo JSON y contiene los datos de los actores y actrices que participaron en la película. El JSON incluye nombres y roles de los miembros del reparto.

•	crew (Lo manejaremos como String pero es JSON):
Esta columna es de tipo JSON y contiene información sobre el equipo de producción de la película, como el director, productor, guionistas, diseñadores de producción, entre otros.

•	ratings (String, JSON):
Esta columna contiene las calificaciones de la película de diferentes fuentes. El valor está representado como una cadena de texto que contiene un JSON con las calificaciones de IMDb, Rotten Tomatoes, Metacritic y otras plataformas relevantes.

# ProyectoIntegrador_PFR
Para la limpieza de los datos se usaron diferentes fromas, por ejemplo en mi caso, usé una carpeta llamada Utils en la cual tengo el código necesario para limpiar los datos:

![image](https://github.com/user-attachments/assets/2bc60660-099a-4272-a911-435a0e29c30a)

```Scala
  def cleanJson(jsonStr: String): Option[String] = {
    try {
      val fixedJsonStr = jsonStr
        .trim
        .replaceAll("'", "\"") // Comillas simples por dobles
        .replaceAll("None", "null") // None -> null
        .replaceAll("True", "true") // Corrige booleanos
        .replaceAll("False", "false")

      // Intenta parsear el JSON
      val json = Json.parse(fixedJsonStr)

      // Devuelve el JSON formateado correctamente
      Some(Json.stringify(json))
    } catch {
      case _: Throwable => None // Retorna None si el JSON está mal formado
    }
  }

  def cleanedDate(value: String): String = {
    val date = General.cleanString(value)
    if (date.matches("""\d{4}-\d{2}-\d{2}""")) date else "0000-01-01"
    value.replaceAll("\"([^\"]*)\"", "\"$1\"")
  }

  def cleanString(value: String): String = {
    if (value == null || value.trim.isEmpty) "Incomplete" else value.trim.replaceAll("\\s+", " ")
  }


  def movies(): List[movies] = {
    val pathDataFile = "data/pi_movies_complete.csv"
    new File(pathDataFile).readCsv[List, movies](rfc.withHeader(true).withCellSeparator(';'))
      .collect {
        case Right(movie) => movie
      }.toList
  }
  def path(name: String): String = {
    s"data/${name}.csv"
  }
```
Para el manejo de la columna crew, en mi caso staff, fue de la siguiente forma: 

Case class:
```Scala
package Models

case class staff(
                 id: Int, //PK
                 name: Option[String],
                 gender: Option[Int],
                 profile_path: Option[String]
               )

```
Código:
```Scala
package limpiezaJson
import java.io.File
import kantan.csv._
import kantan.csv.ops._
import kantan.csv.generic._
import play.api.libs.json.Json

import  Utils.General
import Models.movies
import Models.staff
object Staff extends App{

  implicit val format = Json.format[staff]
  // Leer el archivo CSV y convertirlo a una lista de películas
  val movies = General.movies()
  val staffProcessed = movies.flatMap { movie =>
    try {
      val cleanJson = General.cleanJson(movie.crew).getOrElse("")

      if (cleanJson.startsWith("{")) {
        // Caso donde el JSON es un objeto (una sola colección)
        val json = Json.parse(cleanJson)
        val staff = json.as[staff]
        List(
          (
            staff.id,
            General.cleanString(staff.name.getOrElse("Incompleto")),
            staff.gender.getOrElse(0),
            General.cleanString(staff.profile_path.getOrElse("Incompleto"))
          )
        )
      } else if (cleanJson.startsWith("[") && cleanJson.endsWith("]")) {
        val json = Json.parse(cleanJson)
        val atributos = json.as[List[staff]]  
        atributos.map { staffs =>
          (
            staffs.id,
            General.cleanString(staffs.name.getOrElse("Incompleto")),
            staffs.gender.getOrElse(0),
            General.cleanString(staffs.profile_path.getOrElse("Incompleto"))

          )
        }
      }else{
        None
      }
    } catch {
      case _: Exception => None
    }
  }

  val writer = new File(General.path("staff")).asCsvWriter[(Int, String, Int, String)](rfc.withHeader("id", "name", "gender", "profile_path" ))
  writer.write(staffProcessed).close()
}
```
Para los campos que tengan un valor null, se usa un valor por defecto:

* Cadenas de texto => "Incompleto".
* Valores numéricos => 0.

El código incluye condiciones de validación para determinar si la estructura del JSON es un objeto único o una lista de objetos:

* Si la cadena comienza con '{', significa que el JSON representa un único objeto, por lo que se procesa individualmente.
* Si la cadena comienza con '[' y termina con ']', significa que el JSON es una lista de objetos, por lo que se itera sobre cada elemento para extraer la información.

Esto permite parsear correctamente el JSON y convertirlo en una lista de tuplas con los datos extraídos.

En el caso de la población de datos, tengo 2 formas de hacerlo:
1. Uso del case class:
```Scala
package InsertAll

import doobie._
import doobie.implicits._
import cats.effect.IO
import cats.implicits._

import Config.Database

import Models.actor
object InsertActors {
  def insertActors(actor: actor): ConnectionIO[Int] = {
    sql"""
    INSERT INTO actors (id, name, gender, profile_path) VALUES (${actor.id}, ${actor.name}, ${actor.gender}, ${actor.profile_path})
  """.update.run
  }

  def insertAllActors(datos: List[actor]): IO[List[Int]] = {
    // Filtramos los duplicados en memoria antes de insertarlos
    val uniqueUsers = datos.distinctBy(_.id)

    Database.transactor.use { xa =>
      uniqueUsers.traverse { actor =>
        insertActors(actor).transact(xa).attempt.flatMap {
          case Left(err) =>
            IO(println(s"Error al insertar genero ${actor.id}: ${err.getMessage}")) *> IO.pure(0)
          case Right(inserted) =>
            IO.pure(inserted)
        }
      }
    }
  }

  def getAll: IO[List[actor]] = {
    val query = sql"""
    SELECT * FROM actors
  """.query[actor].to[List]

    Database.transactor.use { xa =>
      query.transact(xa)
    }
  }
}

```
Para este caso tenemos el uso de las librerias doobie._ y doobie.implicits._ las cuales son necesarias para manejas consultas SQL.
* Config.Database: Esto define un transactor para establecer la conexión con la base de datos.
* Importación del modelo actor: Es el case class donde son establecidos los atributos.
* Inset: Se ejecuta la consulta SQL para poder insertar los datos, los cuales se toman del case class actor.
2. Extracción directa del csv:
```Scala
package InsertAll

import com.typesafe.config.ConfigFactory

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}
import scala.io.Source

object InsertMoviesActors extends App {
  val csvFilePath = "data/movie_actor.csv"
  val config = ConfigFactory.load() // Cargar el archivo application.conf

  val url = config.getString("db.url")
  val user = config.getString("db.user")
  val password = config.getString("db.password")

  var connection: Connection = null
  var statement: PreparedStatement = null

  try {
    connection = DriverManager.getConnection(url, user, password)
    val bufferedSource = Source.fromFile(csvFilePath)
    val lines = bufferedSource.getLines().drop(1) // Omitir encabezado

    // Crear una lista de tuplas (actor_id, movie_id, orders, cast_id, credit_id, characters)
    val records = lines
      .map(line => {
        val values = line.split(",").map(_.trim)
        (
          values(0).toInt,  // actor_id
          values(1).toInt,  // movie_id
          values(2).toInt,  // orders
          values(3).toInt,  // cast_id
          values(4),        // credit_id
          values(5)         // characters
        )
      })
      .toList

    // Usar distinctBy para asegurarse de que los registros sean únicos por (movie_id, actor_id)
    val distinctRecords = records.distinctBy(record => (record._2, record._1))

    // Consulta SQL para insertar datos
    val insertQuery = "INSERT INTO movies_actor (actor_id, movie_id, orders, cast_id, credit_id, characters) VALUES (?, ?, ?, ?, ?, ?)"
    statement = connection.prepareStatement(insertQuery)

    // Insertar los registros únicos
    distinctRecords.foreach { record =>
      try {
        statement.setInt(1, record._1) // actor_id
        statement.setInt(2, record._2) // movie_id
        statement.setInt(3, record._3) // orders
        statement.setInt(4, record._4) // cast_id
        statement.setString(5, record._5) // credit_id
        statement.setString(6, record._6) // characters
        statement.executeUpdate()
      } catch {
        case e: SQLException if e.getMessage.contains("foreign key constraint fails") =>
          println(s"ERROR: No se encontró el actor_id=${record._1} o movie_id=${record._2}")
        case e: Exception =>
          println(s"Error al insertar registro: $record")
          e.printStackTrace()
      }
    }

    bufferedSource.close()
    println("Datos insertados correctamente.")
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    if (statement != null) statement.close()
    if (connection != null) connection.close()
  }
}

```
* ConfigFactory.load(): Carga la configuración desde application.conf
* DriverManager: Se usa para conectarse a la base de datos
* Source.fromFile: Se usa para leer el archivo CSV línea por línea
Los primeros pasos que se realizan son:
* La definicion de la ruta del csv y obtengo las credenciales para realizar la conección con la base de datos:
```Scala
  val csvFilePath = "data/movie_actor.csv"
  val config = ConfigFactory.load() // Cargar el archivo application.conf

  val url = config.getString("db.url")
  val user = config.getString("db.user")
  val password = config.getString("db.password")
```
* Apertura de conexión a la base de datos:
```Scala
  var connection: Connection = null
  var statement: PreparedStatement = null

  try {
    connection = DriverManager.getConnection(url, user, password)
```
Se inicializan connection y statement como null y usa DriverManager.getConnection para establecer la conexión con la base de datos.
* Lectura del csv:
```Scala
    val bufferedSource = Source.fromFile(csvFilePath)
    val lines = bufferedSource.getLines().drop(1) // Omitir encabezado
```
Se abre el csv y se lee todas las lineas omitiendo la cabecera (la primera linea)
* Procesamiento de las lineas
```Scala
    // Crear una lista de tuplas (actor_id, movie_id, orders, cast_id, credit_id, characters)
    val records = lines
      .map(line => {
        val values = line.split(",").map(_.trim)
        (
          values(0).toInt,  // actor_id
          values(1).toInt,  // movie_id
          values(2).toInt,  // orders
          values(3).toInt,  // cast_id
          values(4),        // credit_id
          values(5)         // characters
        )
      })
      .toList
```
Cada línea es dividida por una ',' para extraer los valores luego se convierten los valores a los tipos adecuados (Int o String) y se guardan los datos como una lista de tuplas
* Eliminacion de duplicados:
```Scala
 val distinctRecords = records.distinctBy(record => (record._2, record._1))
```
En esta parte de eliminar los duplicados es para que al momento de insertar los datos no ocurran problemas con las restricciones que fueron dadas al crear las tablas
* Insercion de datos:
```Scala
    // Insertar los registros únicos
    distinctRecords.foreach { record =>
      try {
        statement.setInt(1, record._1) // actor_id
        statement.setInt(2, record._2) // movie_id
        statement.setInt(3, record._3) // orders
        statement.setInt(4, record._4) // cast_id
        statement.setString(5, record._5) // credit_id
        statement.setString(6, record._6) // characters
        statement.executeUpdate()
      } catch {
        case e: SQLException if e.getMessage.contains("foreign key constraint fails") =>
          println(s"ERROR: No se encontró el actor_id=${record._1} o movie_id=${record._2}")
        case e: Exception =>
          println(s"Error al insertar registro: $record")
          e.printStackTrace()
      }
    }
```
statement.setXXX(index, value) => Asigna valores a los parámetros (?) y executeUpdate() ejecuta la inserción en la base de datos.

Al final se cierra el csv e imprimimos un mensaje indicando que los datos se insertaron correctamente.

# Estadisticas:
1. Movies:
* 3393 peliculas, este es el total de peliculas unicas, sin repetirse 
2. Genres:
* 20 generos, ese es el total de generos unicos, sin repetirse
3. Popularity:
* Total del índice de la popularidad, sumando el índice de popularidad de todas las peliculas es: 12922.423507999989
* Promedio del índice de popularidad, dividiendo el total del índice de la popularidad para la cantidad de peliculas es: 3.722968455200227
* La pelicula con el índice de popularidad más alta tiene: 123.167259 
3. Votes:
* Total de la cantidad de votantes, sumando los votantes de todas las películas es de: 801206 votantes
* Promedio de votantes, dividiendo el total de los votantes por la cantidad de peliculas es de: 230 votantes
* La película con la cantidad de votantes más alta es de: 14075 votantes
* La película con la cantidad de votantes más baja es de: 0 votantes
4. Score:
* Total de la puntuación. sumando las puntuaciones de todas las películas es de: 19711.1 calificaciones
* Promedio de puntuación, dividiendo el total de las puntuaciones por el total de las películas es de: 5.7 calificaciones
* La película con mayor puntuación es de: 10.0
* Cantida de total de votantes, sumando los votantes de todas las pel[iculas: 801206 
* Promedio de votantes: 230
* La película con la cantidad de votantes más alta es de: 14075 votantes
5. Revenue:
* Total de ingresos generados, sumando los ingresos de todas las películas es: $78975396914
* Promedio de ingresos generados, dividiendo el total de ingresos por la cantidad total de películas registradas: $78975396914
* Película con la mayor cantidad de ingresos registrada: $1118888979

![image](https://github.com/user-attachments/assets/18c04868-90ce-42d6-b4ad-ef183366fa70)


