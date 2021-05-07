# Anserini-Spark Integration

Exploratory integration between Spark and Anserini: provides the ability to "map" over documents in a Lucene index.
This package works with all the versions of the dependencies referenced in [`pom.xml`](pom.xml); in particular, the important ones are Spark (3.1.1), Scala (2.12.0), and Anserini (0.12.0).
Note that Spark still requires Java 11+.

Build the repo:

```
$ mvn clean package
```

## Scala Spark

Here's a demo manipulating the Robust04 collection.
First, fire up Spark shell (adjust memory as appropriate):

```bash
$ spark-shell --jars target/anserini-spark-0.0.1-SNAPSHOT-fatjar.jar --driver-memory 128G
```

Try the following:

```scala
import io.anserini.spark._
import org.apache.spark.rdd.RDD
import java.util.HashMap

val indexPath = "../anserini/lucene-index.robust04.pos+docvectors+rawdocs/"
val docids = new IndexLoader(sc, indexPath).docids
```
You can replace the above `indexPath` variable with any valid index that you have built, for example MS MARCO passage. Typically, you can locate the index under the `anserini/indexes/` directory once you build it.

The value `docids` has type `DocidRDD`, so you now have the full power of Spark.
For example:

```scala
// Project the columns.
val docs1 = docids.docs(doc => (doc.getField("id").stringValue(), doc.getField("raw").stringValue()))
val r1 = docs1.filter(t => (t._2.contains("Albert Einstein"))).collect()

// Grab entire document as a HashMap.
val docs2 = docids.docs()
// docs2 has type org.apache.spark.api.java.JavaRDD[java.util.HashMap[String,String]]

val docs2 : RDD[HashMap[String,String]]  = docids.docs()
// Invoke implicit conversion to Scala RDD.

val r2 = docs2.filter(t => (t.get("raw").contains("Albert Einstein"))).collect()
```

## PySpark

PySpark interfaces with the JVM via the Py4J project, which plays much nicer with Java than Scala.
Here's the equivalent demo in PySpark.
First:

```bash
$ pyspark --jars target/anserini-spark-0.0.1-SNAPSHOT-fatjar.jar --driver-memory 128G
```

The equivalent script to above:

```python
# Import Java -> Python converter
from pyspark.mllib.common import _java2py

INDEX_PATH = "../anserini/lucene-index.robust04.pos+docvectors+rawdocs/"
index_loader = spark._jvm.io.anserini.spark.IndexLoader(spark._jsc, INDEX_PATH)
docids = index_loader.docids()

# Convert to Python.
docs = _java2py(spark.sparkContext, docids.docs())
```

After the above, `docs` is now an RDD in Python.
So we can do stuff like the following:

```python
sample = docs.take(10)

matches = docs.filter(lambda d: 'Albert Einstein' in d['raw']).collect()
```
