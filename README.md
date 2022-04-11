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
La Spark Application, que es un driver program que es responsable de orquestar operaciones paralelas en el cluster Spark, accede a los componentes distribuidos del clsuter. El Driver accede a los componentes distribuidos del cluster (los Spark executors y el cluster manager) através de un *SparkSession*.

#### Spark Driver
Es la parte de la Spark application responsable de instanciar un *SparkSession*. El Spark Sriver tiene diversas funciones como:
- Comunicarse con el administrador (manager) del cluster
- Pedir recuroso como CPU o memoria al manager para los ejecutores de Spark (JVMs)
- Transformar todas las operaciones de Spark en cálculos DAG y los distribuye su ejecución como tareas a los executors.

#### SparkSession
Es un conducto unificado para las operaciones y datos de Spark que ha hecho más fácil trabajar con Spark.
A través del SparkSesion

#### Cluster Manager
El cluster manager es el responsable de administrar y asignar recursos para los nodos del cluster en los que se ejecuta su aplicación Spark. Actuamente Spark admite 4 clusters managers:
- El administrador de clusters independiente integrado
- Apache Hadoop YARNApache Mesos
- Kubernetes

#### Spark executor
Se ejecuta en cada nodo worker del cluster y se comunica con el driver program. Además, son los encargados de ejecutar tareas sobre los workers. Se suele ejecutar solo un executor por nodo.

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
*README.md* 
contiene instrucciones de cómo utilizar las shells de Spark, compilar Spark desde el origen, ejecutar ejemplos de Spark...

#### Directorios y archivos de Spark
*bin* 
Contiene la mayoría de los scripts para interactuar con Spark incluyendo Spark shells (spark-sql, pyspark, spark-shell y sparkR)

*sbin*
Los scripts de este directorio tienen un propósito más administrativo, para inciar y detener los compomentes de Spark en el cluster.

*kubernetes*
Contienen Dockerfiles para crear imágenes Docker

*data*
Este directorio está `pblacon con archivos .txt

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
Algunos términos importantes
