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

### Spark SQL
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

