package io.anserini.spark

import java.util.stream.{Collectors, IntStream}
import java.util.{ArrayList, HashMap, Iterator}

import io.anserini.hadoop.HdfsReadOnlyDirectory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.lucene.document.Document
import org.apache.lucene.index.DirectoryReader
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object IndexLoader {

  def docs[T: ClassTag](rdd: JavaRDD[Integer], path: String, extractor: Document => T): JavaRDD[T] = {
    rdd.mapPartitions(new FlatMapFunction[Iterator[Integer], T] {
      override def call(iter: Iterator[Integer]): Iterator[T] = {
        val reader = DirectoryReader.open(new HdfsReadOnlyDirectory(new Configuration(), new Path(path)))
        val list = new ArrayList[T]()
        while (iter.hasNext) {
          list.add(extractor(reader.document(iter.next)))
        }
        list.iterator()
      }
    })
  }

  // Used in PySpark... need to return results as a HashMap since a Lucene's Document can't be serialized
  def docs2map(rdd: JavaRDD[Integer], path: String): JavaRDD[HashMap[String, String]] = docs(rdd, path, doc => {
    val map = new HashMap[String, String]()
    for (field <- doc.getFields.asScala) {
      map.put(field.name(), field.stringValue())
    }
    map
  });

}

/**
  * Load document IDs from a Lucene index
  */
class IndexLoader(sc: JavaSparkContext, path: String) {

  /**
    * Default Hadoop Configuration
    */
  val config = new Configuration()

  /**
    * Lucene IndexReader
    */
  val reader = DirectoryReader.open(new HdfsReadOnlyDirectory(config, new Path(path)))

  /**
    * Get the document IDs
    *
    * @return an RDD of document IDs
    */
  def docids(): JavaRDD[Integer] = {
    docids(numDocs() - 1)
  }

  /**
    * Get the document IDs up to a certain num
    *
    * @param num the limit of IDs
    * @return an RDD of document IDs
    */
  def docids(num: Integer): JavaRDD[Integer] = {
    // sc.parallelize(IntStream.rangeClosed(0, num).boxed().collect(Collectors.toList()))
    // This doesn't work for Java 11: Static methods in interface require -target:jvm-1.8
    sc.parallelize(0 to num toList).map(i => i : java.lang.Integer).toJavaRDD()
  }

  /**
    * Get the number of documents in the index.
    *
    * @return the number of documents in the index
    */
  def numDocs(): Integer = {
    reader.numDocs()
  }

}