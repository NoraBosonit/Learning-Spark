# Learning-Spark
## Chapter 1: Introduction to Apache Spark
Hadoop MP se quedó corto para tareas de ML o consultas de SQL por lo que surge el ecosistema Hadoop. 
Además, MP era ineficiente para procesos iterativos. Surge Apache Spark

### Qué es?
Apache Spark es un motor unificado diseñado para el procesamiento de datos distribuidos a gran escala en las insalaciones de los centros de datos o en la nube

Se centra en 4 características:
- Velocidad
- Facilidad de uso
- Modularidad
- Extensibilidad

### Análisis Unificado
#### Spark SQL
Funciona bien con datos estucturados y puede leer datos almacenados en tablas RDBMS o desde archivos estructurasos como CSV, text, JSON... y construir tablas permanentes o temporales en Spark. 

Ejemplo de lectura desde JSON
```
// In Scala
// Read data off Amazon S3 bucket into a Spark DataFrame
spark.read.json("s3://apache_spark/data/committers.json")
 .createOrReplaceTempView("committers")
// Issue a SQL query and return the result as a Spark DataFrame
val results = spark.sql("""SELECT name, org, module, release, num_commits
 FROM committers WHERE module = 'mllib' AND num_commits > 10
 ORDER BY num_commits DESC""")
```

#### Spark MLlib
Es una bibliotecta que contiene algoritmos para el Aprendizaje Automático. Se divide en 2 partes:
- spark.mllib: basada en DataFrame
- spark.ml: basada en RDD

Ejemplo
```
# In Python
from pyspark.ml.classification import LogisticRegression
...
training = spark.read.csv("s3://...")
test = spark.read.csv("s3://...")
# Load training data
lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
# Fit the model
lrModel = lr.fit(training)
# Predict
lrModel.transform(test)
...

```

#### Transmisión estructurada de Spark
Necesario para que los desarrolladores puedan trabajar tanto con datos estáticos como con datos de transmisión de motores

Ejemplo
```
# In Python
# Read a stream from a local host
from pyspark.sql.functions import explode, split
lines = (spark
 .readStream
 .format("socket")
 .option("host", "localhost")
 .option("port", 9999)
 .load())
# Perform transformation
# Split the lines into words
words = lines.select(explode(split(lines.value, " ")).alias("word"))
# Generate running word count
word_counts = words.groupBy("word").count()
# Write out to the stream to Kafka
query = (word_counts
 .writeStream
 .format("kafka")
 .option("topic", "output"))
```

#### GraphX
Es una biblioeca para manipular grafos

Ejemplo
```
// In Scala
val graph = Graph(vertices, edges)
messages = spark.textFile("hdfs://...")
val graph2 = graph.joinVertices(messages) {
 (id, vertex, msg) => ...
}

// In Python (creo)
from -- import -- as-- x
graph = x.Graph(vertices, edges)
.
.
.
```

### Ejecución distribuida de Apache Spark
Los componentes de Apache Spark funcionan en colaboración dentro de un grupo de máquinas para obtener un procesamiento distribuido de los datos. Esto sucede de la siguiente forma:
La Spark Application, el cual es un driver program que es responsable de orquestar operaciones paralelas en el cluster Spark, accede a los componentes distribuidos del clsuter. El Driver accede a los componentes distribuidos del cluster (los Spark executors y el cluster manager) através de un *SparkSession*.

#### Spark Driver
Es la parte de la Spark application responsable de instanciar un *SparkSession*. El Spark Sriver tiene diversas funciones como:
- Comunicarse con el administrador (manager) del cluster
- Pedir recuroso como CPU o memoria al manager para los ejecutores de Spark (JVMs)
- Transformar todas las operaciones de Spark en cálculos DAG y los distribuye su ejecución como tareas a los executors.

#### SparkSession
Es un conducto unificado para las operaciones y datos de Spark que ha hecho más fácil trabajar con Spark. Es el punto de entrada a Spark SQL y se necesita para hacer operaciones en Spark, es lo primero que se crea. Una vez se ha creado la SparkSession, se pueden definir data frames, emitir consultas SQL...

En la spark-shell la SparkSession se crea automáticamente y puedes acceder a ella mediante una variable llamada *spark* o *sc*. 

Ejemplo

```
// In Scala
import org.apache.spark.sql.SparkSession
// Build SparkSession
val spark = SparkSession
 .builder
 .appName("LearnSpark")
 .config("spark.sql.shuffle.partitions", 6)
 .getOrCreate()
...
// Use the session to read JSON
val people = spark.read.json("...")
...
// Use the session to issue a SQL query
val resultsDF = spark.sql("SELECT city, pop, state, zip FROM table_name")

```

#### Cluster Manager
El cluster manager es el responsable de administrar y asignar recursos para los nodos del cluster en los que se ejecuta su aplicación Spark. Actuamente Spark admite 4 clusters managers:
- El administrador de clusters independiente integrado
- Apache Hadoop YARN
- Apache Mesos
- Kubernetes

#### Spark executor
Se ejecuta en cada nodo worker del cluster y se comunica con el Driver program. Además, son los encargados de ejecutar tareas sobre los workers. Se suele ejecutar solo un executor por nodo.

#### Modos de implementación
Spark es compatible con innumerables modos de implementación lo que permite que se pueda ejecutar en diferentes congiguraciones y entornos. Se puede configurar en Apache Hadoop YARN y Kubernetes.

#### Distributed data and partitions
Los datos se dividen en particiones y se reparten por todo el cluster para crear un paralelismo a la hora de ejecutar las tareas.

Ejemplo

Este código divide los datos físicos almacenados en los clusters en 8 particiones.
```
# In Python
log_df = spark.read.text("path_to_large_text_file").repartition(8)
print(log_df.rdd.getNumPartitions())
```
Aquí se creará un DataFrame de 10.000 enteros distribuidos en 8 particiones de memoria. 
```
# In Python
df = spark.range(0, 10000, 1, 8)
print(df.rdd.getNumPartitions())
```

## Chapter 2: Downloading and Getting Started
El modo local para grandes conjuntos de datos no es adecuado. Mejor YARN o Kubernetes. 

### Step 1: Downloading Apache Spark
#### Directorios y archivos de Spark
*README.md* 
Contiene instrucciones de cómo utilizar las shells de Spark, compilar Spark desde el origen, ejecutar ejemplos de Spark...

*bin* 
Contiene la mayoría de los scripts para interactuar con Spark incluyendo Spark shells (spark-sql, pyspark, spark-shell y sparkR)

*sbin*
Los scripts de este directorio tienen un propósito más administrativo, para inciar y detener los compomentes de Spark en el cluster.

*kubernetes*
Contienen Dockerfiles para crear imágenes Docker

*data*
Este directorio está poblacon con archivos *.txt*

### Paso 2: Uso de Scala o PySpark shell
Ejemplo en el que se lee un archivo de texto como un DataFrame, se muestra una muestra de cadenas vacías y se cuenta el número total de lineas en el archivo.
show(10, flase) muestra las 10 primeras lineas sin truncar (false por defecto true)

```
With Python
>>> strings = spark.read.text("../README.md")
>>> strings.show(10, truncate=False)
+------------------------------------------------------------------------------+
|value |
+------------------------------------------------------------------------------+
|# Apache Spark |
| |
|Spark is a unified analytics engine for large-scale data processing. It |
|provides high-level APIs in Scala, Java, Python, and R, and an optimized |
|engine that supports general computation graphs for data analysis. It also |
|supports a rich set of higher-level tools including Spark SQL for SQL and |
|DataFrames, MLlib for machine learning, GraphX for graph processing, |
|and Structured Streaming for stream processing. |
| |
|<https://spark.apache.org/> |
+------------------------------------------------------------------------------+
only showing top 10 rows
>>> strings.count()
109
>>>
```

### Paso 3: Comprender los conceptos de la aplicación Spark
Algunos términos importantes:

*Application*
Es un programa de usuario creado en Spark utilizando sus API. Es un proograma controlador y ejecutor del cluster.

*SparkSession*
Es un objeto que proporciona un punto de entrada para programar con las API de Spark.

*Job*
Es un cómputo paralelo que consta de varias tareas que se generan en respuesta a una acción de Spark.

*Stage*
Cada job se divide en conjuntos más pequeños de tasks llamadas stages que dependes unas de las otras.

*Task*
Una única unidad de trabajo o ejecución que se enviará a un Spark executor

#### Aplicación Spark y SparkSession
En el nucleo de cada Application Spark se encuantra el driver program que crea un objeto del tipo SparkSession. Cuando se trabaja con la shell de spark, la SparkSession se crea directamente sin necesidad de hacer nada.

#### Spark Jobs
Durante las sesiones con las Spark shells, el driver convierte la Spark application en uno o más jobs. Luego transforma cada job en un DAG. Este es el plan de ejecución de Spark donde cada nodo dentro de un DAG podría ser una o varias etapas de Spark.

#### Spark Stages
Las Stages se crean en función de qué operaciones se pueden realizar en serie o en paralelo. No todas las operaciones pueden ocurrir en una sola etapa (stage), por lo que se dividen en varias. 

#### Spark Tasks
Cada stage se compone de tareas de Spark que se llevan a cabo en cada executor de Spark. Cada tarea se asigna a un solo nucleo y funciona en una sola particion de datos. 

### Transformaciones, acciones y evaluación perezosa
Las transformaciones transforman un Data Frame en otro sin alterar los datos originales, dando como resultado la inmutabilidad de los dataframes y, por tanto, la tolerancia a fallos. Las transformaciones se evalúan perezosamente, es decir, los resultados no se conmutan inmediatamente sino que se espera a qua se ejecute una acción para ññevar a cabo las transformaciones. 

Ejemplos de acciones y transformaciones

| Transformaciones| Acciones|
| ----- | ---- |
|orderBy()|show()|
|groupBy()|take()|
|filter()|count()|
|select()|collect()|
|join()|save()|

Ejemplo
```
# In Python 
>>> strings = spark.read.text("../README.md") 
>>> filtered = strings.filter(strings.value.contains("Spark")) 
>>> filtered.count() 
20 
```

En este caso hay 2 transfromaciones; read() y filter(), y una acción; count(). No se desencadena la ejecución hasta que count() no se ejecuta en el shell.

#### Transformaciones estrechas y anchas
El que Spark siga un esquema perezoso es que puede inspeccionar su consulta y determinar cómo llevarla a cabo de forma óptima; uniendo operaciones y asignándolas a una sola etapa o mezclado de datos y división entre clusters. 

Las transformaciones se pueden dividir en:

- dependencias estrechas
- dependencias anchas

Las dependencias estrechas son aquellas con las que se puede calcular una sola partición de salida a partir de una sola partición de entrada sin intercambio de datos entre particiones. Por ejemplo, filter() y contains().

Las dependecias anchas leen datos, los transforman y hacen la lectura en el disco. Un ejemplo es groupBy() que tiene que modificar la tabla original para mostrar el resultado. Se necesita una combinación entre particiones. 


### Interfaz de usuario de Spark
- Se ejecuta en el puerto 4040
En imágenes del libro se ve con más detalle

(falta poner imágemes job, stage, task e interfaz)


## Chapter 3: Apache Spark’s Structured APIs
### Spark: ¿Qué hay debajo de un RDD?
El RDD es la abstracción más básica de Spark el cual tiene asociado 3 características:

- Dependecias 
- Particiones (con información de la localización)
- Compute function (Partition => Iterador[T] #Creo que es en Scala

La lista de dependencias indica a Spark cómo se construye un RDD con sus entradas. Cuando sea necesario para reproducir resultados, Spark puede recrear un RDD a partir de esas dependecias y replicar operaciones. Esta característica le da resiliencia a los RDD.
Las particiones le dan a Spark la capacidad de dividir el trabajo para paralelizar el cálculo entre los executers.
La función de cálculo produce un iterador apra los datos que serán almacenados en el RDD.

Sin embargo, hay una abstracción que produce un desconocimiento de Spark de todo lo que está haciendo en la función de cómputo. Además de que el iterador también es opaco, lo ve como un objeto genérico de Python.  

Por todo esto surge Spark 2.x, un Spark estructurado.

### Structuring Spark
Spark 2.x introdujo algunos esquemas clave para estructurar Spark utilizando patrones para expresar los cálculos. Estos patrones se expresan con operaciones como contar, seleccionar, filtrar, agregar, promediar, agregar... Todo esto permite decirle a Spark qué es lo que se quiere hacer y como resultado un plan eficiente para su ejecución dando como resultado una estructura que le permite organizar los datos en formato de tabla SQL u hoja de cálculo.

#### Méritos y beneficios clave
Esta estructura produce unos beneficios como un mejor rendimiento y una eficiencia de espacio en todos los componentes de Spark. Las ventajas más importantes son:

- Expresividad
- Simplicidad
- Compatibilidad
- Uniformidad

Las diferencias mecionadas se pueden ver aquí:

Abstracción RDD

```
# In Python
# Create an RDD of tuples (name, age)
dataRDD = sc.parallelize([("Brooke", 20), ("Denny", 31), ("Jules", 30),
 ("TD", 35), ("Brooke", 25)])
# Use map and reduceByKey transformations with their lambda 
# expressions to aggregate and then compute average
agesRDD = (dataRDD
 .map(lambda x: (x[0], (x[1], 1)))
 .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
 .map(lambda x: (x[0], x[1][0]/x[1][1])))
```

Spark 2.x

```
# In Python 
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
# Create a DataFrame using SparkSession
spark = (SparkSession
 .builder
 .appName("AuthorsAges")
 .getOrCreate())
# Create a DataFrame 
data_df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30),
 ("TD", 35), ("Brooke", 25)], ["name", "age"])
# Group the same names together, aggregate their ages, and compute an average
avg_df = data_df.groupBy("name").agg(avg("age"))
# Show the results of the final execution
avg_df.show()
+------+--------+
| name|avg(age)|
+------+--------+
|Brooke| 22.5|
| Jules| 30.0|
| TD| 35.0|
| Denny| 31.0|
+------+--------+

```

En ambos casos se está agregando las edades de cada nombre, se agrupan por nombre y se calcula la media de las edades. Sin embargo, mientras que en el primer caso es muy difícil de leer, en el segundo se puede ver un código más expresivo y simple. Esto es debido a que se está utilizando operadores DSL de alto nivel para decirle a Spark qué hacer. De esta forma puede optimizar su consulata y ejecutar las consulatas de forma eficiente. 


### The DataFrame API
Inspirado en el DataFrame de pandas en su estructura, formato y algunas peraciones específicas. 
Cuando los datos se visualizan como una tabla estructurada no solo son más fáciles de ver sino también más fáciles de trabajar con ellos.

#### Tipos de datos básicos de Spark
Con todos los lenguajes de programación admitidos, Spark soporta tipos de datos internos básicos. Estos tipos de datos pueden declararse en su aplicación Spark o definirse en su esquena. En Scala se puede definir o declarar un nombre de columna en partitular. 

```
$SPARK_HOME/bin/spark-shell
scala> import org.apache.spark.sql.types._
import org.apache.spark.sql.types._
scala> val nameTypes = StringType
nameTypes: org.apache.spark.sql.types.StringType.type = StringType
scala> val firstName = nameTypes
firstName: org.apache.spark.sql.types.StringType.type = StringType
scala> val lastName = nameTypes
lastName: org.apache.spark.sql.types.StringType.type = StringTyp
```

Estos son los tipos de datos básicos de Python en Spark:




| DataType|Value assigned in Python| API to instanciate |
| ------ | -------- | --------|
|ByteType | int | DataTypes.ByteType|
|ShortType | int | DataTypes.ShortType|
|IntegerType | int | DataTypes.IntegerType|
|LongType | int | DataTypes.LongType|
|FloatType | float | DataTypes.FloatType|
|DoubleType | float | DataTypes.DoubleType|
|StringType | str | DataTypes.StringType|
|BooleanType | bool | DataTypes.BooleanType|
|DecimalType | decimal.Decimal | DecimalType|



#### Tipos de datos complejos y estructurados de Spark
Para los análisis de datos complejos nose trabajará con datos simples o básicos sino datos complejos, normalmente estructurados o anidados. 

Tipos de datos complejos de Python soportados por Spark:

| DataType|Value assigned in Python| API to instanciate |
| ------|--------|---------|
|BinaryType | bytearray | BinaryType()|
|TimestampType | datetime.datetime | TimestampType()|
|DataType | datetime.date | DateType()|
|ArrayType | list, tuple or array | ArrayType(dataType, [nullable])|
|MapType | dict | MapType(keyType, valueType, [nullable])|
|StructType | list or tuple | StructType([fields])|
|StructField | a value type corresponding to the type of this field | StructField(name, dataType, [nullable])|

#### Esquemas y creación de DataFrames
Un esquema en Spark define los nombres de las columnas y los tipos de datos para un DataFrame. Se suele definir el esquema por adelantado y esto ofrece tres beneficios:

- Libera a Spark de la responsabilidad de predecir el tipo de datos
- Evite a Spark crear un esquema de datos que para muchos datos puede ser costoso
- Spark puede detectar errores si los datos no coinciden con el esquema

##### Dos formas de definor un esquema
1. Definirlo programando
```
# In Python
from pyspark.sql.types import *
schema = StructType([StructField("author", StringType(), False),
 StructField("title", StringType(), False),
 StructField("pages", IntegerType(), False)])
```
2. Emplear DDL: más simple y fácil de leer
```
# In Python
schema = "author STRING, title STRING, pages INT"
```

Para muchos ejemplos se utilizarán los dos juntos:

```
# In Python 
from pyspark.sql import SparkSession
# Define schema for our data using DDL 
schema = "`Id` INT, `First` STRING, `Last` STRING, `Url` STRING, 
 `Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING>"
# Create our static data
data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter",
The DataFrame API | 51
"LinkedIn"]],
 [2, "Brooke","Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter",
"LinkedIn"]],
 [3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web",
"twitter", "FB", "LinkedIn"]],
 [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568,
["twitter", "FB"]],
 [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web",
"twitter", "FB", "LinkedIn"]],
 [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568,
["twitter", "LinkedIn"]]
 ]
# Main program
if __name__ == "__main__":
 # Create a SparkSession
 spark = (SparkSession
 .builder
 .appName("Example-3_6")
 .getOrCreate())
 # Create a DataFrame using the schema defined above
 blogs_df = spark.createDataFrame(data, schema)
 # Show the DataFrame; it should reflect our table above
 blogs_df.show()
 # Print the schema used by Spark to process the DataFrame
 print(blogs_df.printSchema())
```

**En libro página 78 ejemplo Scala leyendo datos JSON**

#### Columnas y expresiones
Nombrar columnas en DataFrames es similar a nombrar columnas tablas de pandas, R o RDBMS. Además se pueden crear expresiones usando ```expr("columnName * 5")``` or ```(expr("colum
nName - 5") > col(anothercolumnName)), where columnName is a Spark type (inte‐
ger, string, etc.)```. expr() is part of the pyspark.sql.functions (Python) and
org.apache.spark.sql.functions (Scala) packages.

Tanto en Scala como en Java y Python hay métodos asociados con las columnas: ```col()``` y ```Column```. Siendo el primero el que devuelve el objeto y el segundo el nombre del objeto.

Ejemplo de expresiones y columnas en Scala

```
// In Scala 
scala> import org.apache.spark.sql.functions._
scala> blogsDF.columns
res2: Array[String] = Array(Campaigns, First, Hits, Id, Last, Published, Url)
// Access a particular column with col and it returns a Column type
scala> blogsDF.col("Id")
res3: org.apache.spark.sql.Column = id
// Use an expression to compute a value
scala> blogsDF.select(expr("Hits * 2")).show(2)
// or use col to compute value
scala> blogsDF.select(col("Hits") * 2).show(2)
+----------+
|(Hits * 2)|
+----------+
| 9070|
| 17816|
+----------+
// Use an expression to compute big hitters for blogs
// This adds a new column, Big Hitters, based on the conditional expression
blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show() #Crea una columa booleana
+---+---------+-------+---+---------+-----+--------------------+-----------+
| Id| First| Last|Url|Published| Hits| Campaigns|Big Hitters|
+---+---------+-------+---+---------+-----+--------------------+-----------+
| 1| Jules| Damji|...| 1/4/2016| 4535| [twitter, LinkedIn]| false|
| 2| Brooke| Wenig|...| 5/5/2018| 8908| [twitter, LinkedIn]| false|
| 3| Denny| Lee|...| 6/7/2019| 7659|[web, twitter, FB...| false|
| 4|Tathagata| Das|...|5/12/2018|10568| [twitter, FB]| true|
| 5| Matei|Zaharia|...|5/14/2014|40578|[web, twitter, FB...| true|
| 6| Reynold| Xin|...| 3/2/2015|25568| [twitter, LinkedIn]| true|
+---+---------+-------+---+---------+-----+--------------------+-----------+
// Concatenate three columns, create a new column, and show the 
// newly created concatenated column
blogsDF #Se concatena nombre apellido e id
 .withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id"))))
 .select(col("AuthorsId")) 
 .show(4)
+-------------+
| AuthorsId|
+-------------+
| JulesDamji1|
| BrookeWenig2|
| DennyLee3|
|TathagataDas4|
+-------------+
// These statements return the same value, showing that
// expr is the same as a col method call
blogsDF.select(expr("Hits")).show(2)
blogsDF.select(col("Hits")).show(2)
blogsDF.select("Hits").show(2)
+-----+
| Hits|
+-----+
| 4535|
| 8908|
+-----+
// Sort by column "Id" in descending order
blogsDF.sort(col("Id").desc).show()
blogsDF.sort($"Id".desc).show()
+--------------------+---------+-----+---+-------+---------+-----------------+
| Campaigns| First| Hits| Id| Last|Published| Url|
+--------------------+---------+-----+---+-------+---------+-----------------+
| [twitter, LinkedIn]| Reynold|25568| 6| Xin| 3/2/2015|https://tinyurl.6|
|[web, twitter, FB...| Matei|40578| 5|Zaharia|5/14/2014|https://tinyurl.5|
| [twitter, FB]|Tathagata|10568| 4| Das|5/12/2018|https://tinyurl.4|
|[web, twitter, FB...| Denny| 7659| 3| Lee| 6/7/2019|https://tinyurl.3|
| [twitter, LinkedIn]| Brooke| 8908| 2| Wenig| 5/5/2018|https://tinyurl.2|
| [twitter, LinkedIn]| Jules| 4535| 1| Damji| 1/4/2016|https://tinyurl.1|
+--------------------+---------+-----+---+-------+---------+-----------------+

In this last example, the expressions blogs_df.sort(col("Id").desc) and
blogsDF.sort($"Id".desc) are identical. They both sort the DataFrame column
named Id in descending order: one uses an explicit function, col("Id"), to return a

Column object, while the other uses $ before the name of the column, which is a func‐
tion in Spark that converts column named Id to a Column.
```

#### Filas
Una fila en Spark es un Row object genérico que contiene una o más columnas donde pueden ser del mismo tipo o de tipos distintos. Se puede acceder a los campos de las filas con índices (de 0 a num_col-1)

```
# In Python
from pyspark.sql import Row
blog_row = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",
 ["twitter", "LinkedIn"])
# access using index for individual items
blog_row[1]
'Reynold'
```

Los objetos fila se pueden utilizar para crear DataFrames si se necesita.

```
# In Python 
rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
authors_df = spark.createDataFrame(rows, ["Authors", "State"])
authors_df.show()

+-------------+-----+
| Author|State|
+-------------+-----+
|Matei Zaharia| CA|
| Reynold Xin| CA|
+-------------+-----+
```

#### Operaciones comunes en DataFrames
Spark ofrece:

- DataFrameReader para leer datos a DataFrame
- DataFrameWriter para guardar DataFrames

##### Utilización de DataFrameReader y DataFrameWriter
Si al leer datos no definimos esquema, Spark lo infiere (en Scala se utiliza la opción ```samplingRatio```. 
En Python, para leer un DataFrame:

```
sf_fire_file = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
```
Siendo fire_schema un esquema ya definido.

Para escribir (o guardar) un DataFrame se utiliza la función DataFrameReader que por defecto guarda los datos en formaro parquet. 

```
# In Python to save as a Parquet file
parquet_path = ...
fire_df.write.format("parquet").save(parquet_path)
```

También se pueden guardar en formato tabla SQL

```
parquet_table = ... # name of the table
fire_df.write.format("parquet").saveAsTable(parquet_table)
```

##### Proyecciones y filtros
Una proyección es una forma de devolver solo las filas que coinciden con una determinada condición relacional mediante el uso de filtros. Las proyecciones se hacen con select() y los filtros con filter() o where().

```
# In Python
few_fire_df = (fire_df
 .select("IncidentNumber", "AvailableDtTm", "CallType")
 .where(col("CallType") != "Medical Incident"))
few_fire_df.show(5, truncate=False)
```

Podemos saber cuántos registros hay como resultado con countDistinct()

```
# In Python, return number of distinct types of calls using countDistinct()
from pyspark.sql.functions import *
(fire_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .agg(countDistinct("CallType").alias("DistinctCallTypes"))
 .show())
```

En vez de mostrar el número, mostramos las filas:

```
(fire_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .distinct()
 .show(10, False))
```

##### Cambiar agregar y eliminar columnas
En Jupyter Notebook.

Una vez hemos cambiado las columnas a tipo fecha, se puede utilizar funciones como month(), year(), day() provenientes de spark.sql.functions. 

```
# In Python
(fire_ts_df
 .select(year('IncidentDate'))
 .distinct()
 .orderBy(year('IncidentDate'))
 .show())
```

##### Agregaciones
¿Qué pasa si queremos saber cuáles fueron los tipos más comunes de llamadas de incendios 
o qué códigos postales representaron la mayoría de las llamadas? Este tipo de preguntas son comunes en el análisis y la exploración de datos.
Funciones como groupBy(), orderBy(), count() ofrecen la capacidad de agrupar y contar. 

- Cuáles fueron los tipos de llamada de incendio más frecuentes?
```
# In Python
(fire_ts_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .groupBy("CallType")
 .count()
 .orderBy("count", ascending=False)
 .show(n=10, truncate=False))
```

##### Otras operaciones comunes de DataFrame
Hay otras funciones como:
- min()
- max()
- sum()
- mean()

Aquí calculamos la suma de alarmas, el tiempo de respuesta promedio y los tiempos de respuesta mínimo y  máximo para todas las llamadas de incendio en nuestro conjunto de datos:

```
# In Python
import pyspark.sql.functions as F
(fire_ts_df
 .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
 F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
 .show())
```

#### End-to-End DataFrame Example
Ejercicios en el Notebook

### The Dataset API
Spark unificó los DataFrames y los dataset de las API como APIs estructuradas con interfaces similares para que los desarrolladores solo tengan que aprender un único conjunto de APIs. Los DataSets tienen dos características:

- typed APIs 
- untyped APIs

#### Typed Objects, Untyped Objects and Generic Rows
En los lenguajes soportados por Spark, los Datasets solo tienen sentido en Java y Scala mientras que en Python y R solo son los DataFrames. 

|Language | Typed and untyped main abstraction | Typed or untyped |
| ------ | -------- | --------|
|Scala | Dataset[T] and DataFrame (alias for Dataset[Row]) | Both typed and untyped|
|Java |Dataset<T>| Typed|
|Python| DataFrame |Generic Row untyped|
|R| DataFrame |Generic Row untyped|
 
 Las filas son objetos genéricos a los que se acceden mediante índices. 
 
 ```
 from pyspark.sql import Row
row = Row(350, True, "Learning Spark 2E", None)
row[0]
row[1]
 ```
