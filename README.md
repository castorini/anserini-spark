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
