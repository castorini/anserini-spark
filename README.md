# Anserini-Spark Integration

Exploratory integration between Spark and Anserini: provides the ability to "map" over documents in a Lucene index.
This package works with all the versions of the dependencies referenced in [`pom.xml`](pom.xml); in particular, the important ones are Spark (3.1.1), Scala (2.12.0), and Anserini (0.12.0).
Note that Spark still requires Java 11+.

**Setup Note:** While cloning this project, make sure to clone it in the same repository where you cloned anserini. Also, before building this project, make sure to build anserini first.

Build the repo:

```
$ mvn clean package
```
**Note:** If you encounter build issues during `compile` with `maven-gpg-plugin:1.6:sign` saying `gpg: no default secret key: No secret key` followed by `gpg: signing failed: No secret key`, try to run `which gpg` followed by `gpg --list-secret`

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
If you would like to convert `docs` from `pyspark.rdd.RDD` to `pyspark.sql.dataframe.DataFrame` to use the operations related to dataframes, then you can do this as follows:

```python
from pyspark.sql import SQLContext

# After docs variable has the PySpark RDD
my_schema = sqlContext.createDataFrame(docs)
my_schema.createOrReplaceTempView("docs")
```

`my_schema` will have the same structure as `docs` with columns as `(id, raw)`

If you want to filter the `raw` column based on a keyword as shown above, the same can be achieved with a dataframe as well:

```python

# Can be used for any keyword
results = my_schema.filter(my_schema.raw.contains('America'))
```
