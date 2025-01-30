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
