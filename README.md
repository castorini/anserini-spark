# Anserini-Spark

Exploratory integration between Spark and Anserini: provides the ability to "map" over documents in a Lucene index.

Build the repo:

```
$ mvn clean package
```

A demo mapping over the Core18 index of the Washington Post collection:

```
$ spark-shell --jars target/anserini-spark-0.0.1-SNAPSHOT-fatjar.jar
import io.anserini.spark._

val indexPath = "../Anserini/lucene-index.core18.pos+docvectors+rawdocs/"
val docids = new IndexLoader(sc, indexPath).docids
val docs = docids.getDocs(indexPath, doc => doc.getField("raw").stringValue())
```

The val `docs` has type:

```
docs: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[1]
```

It's an RDD... so you can now do whatever you want with it.

# PySpark

PySpark interfaces with the JVM via the Py4J project, which plays much nicer with Java than Scala. Thus, the Scala classes used above have been re-written in (roughly) equivelant Java code.

```
$ pyspark --jars target/anserini-spark-0.0.1-SNAPSHOT-fatjar.jar

# Import Java -> Python converter
from pyspark.mllib.common import _java2py

# The path of the Lucene index
INDEX_PATH = "/tuna1/indexes/lucene-index.core17.pos+docvectors+rawdocs/"

# The JavaIndexLoader instance
index_loader = sc._jvm.io.anserini.spark.JavaIndexLoader(sc._jsc, INDEX_PATH)

# Get the document IDs as an RDD
docids = index_loader.docIds()

# Get an instance of our Lucene RDD class
lucene = sc._jvm.io.anserini.spark.JavaLuceneRDD(docids)

# Get the JavaRDD of Lucene Document as a Map (Document can't be serialized)
docs = lucene.getDocs(INDEX_PATH)

# Convert to a Python RDD
docs = _java2py(sc, docs)
```

`docs` is a `<class 'pyspark.rdd.RDD'>`... we can do whatever we want with it now too.
