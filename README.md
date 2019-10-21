# Anserini-Spark Integration

Exploratory integration between Spark and Anserini: provides the ability to "map" over documents in a Lucene index.
This package works with all the versions of the dependencies referenced in [`pom.xml`](pom.xml); in particular, the important ones are Spark (2.11.12), Scala (2.4.4), and Anserini (0.6.0).
Note that Spark still requires Java 8.

Build the repo:

```
$ mvn clean package
```

## Scala Spark

Here's a demo mapping manipulating the Robust04 collection.
Fire up Spark shell (adjust memory as appropriate):

```bash
$ spark-shell --jars target/anserini-spark-0.0.1-SNAPSHOT-fatjar.jar --driver-memory 128G
```

Try the following:

```scala
import io.anserini.spark._

val indexPath = "../anserini/lucene-index.robust04.pos+docvectors+rawdocs/"
val docids = new IndexLoader(sc, indexPath).docids
val docs = docids.docs(indexPath, doc =>
    (doc.getField("id").stringValue(), doc.getField("raw").stringValue()))
```

The val `docs` has type `JavaRDD`.
To convert to an `RDD`:

```scala
val rdd = org.apache.spark.api.java.JavaRDD.toRDD(docs)
```

It's now an RDD... so you can now do whatever you want with it.
For example:

```scala
rdd.filter(t => t._2.contains("Albert Einstein")).count()
// There are 65.

val samples = rdd.filter(t => t._2.contains("Albert Einstein"))
  .map(t => Tuple2(t._1, t._2)).collect()
```

The extra `map` is to deal with weird Java to Scala conversion issues.

## PySpark

PySpark interfaces with the JVM via the Py4J project, which plays much nicer with Java than Scala. Thus, the Scala classes used above have been re-written in (roughly) equivelant Java code.

```
$ pyspark --jars target/anserini-spark-0.0.1-SNAPSHOT-fatjar.jar

# Import Java -> Python converter
from pyspark.mllib.common import _java2py

# The path of the Lucene index
INDEX_PATH = "../anserini/lucene-index.core18.pos+docvectors+rawdocs/"

# The JavaIndexLoader instance
index_loader = sc._jvm.io.anserini.spark.IndexLoader(sc._jsc, INDEX_PATH)

# Get the document IDs as an RDD
docids = index_loader.docids()

# Get the JavaRDD of Lucene Document as a Map (Document can't be serialized)
docs = lucene.docs2map(INDEX_PATH)

# Convert to a Python RDD
docs = _java2py(sc, docs)
```

`docs` is a `<class 'pyspark.rdd.RDD'>`... we can do whatever we want with it now too.
