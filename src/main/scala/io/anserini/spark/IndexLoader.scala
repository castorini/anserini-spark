package io.anserini.spark

import java.util.HashMap

import io.anserini.hadoop.HdfsReadOnlyDirectory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.lucene.document.Document
import org.apache.lucene.index.DirectoryReader
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
  * Main entry point for accessing a Lucene index.
  */
class IndexLoader(sc: JavaSparkContext, path: String) {
// Note that we're taking in JavaSparkContext (to support PySpark),
// but relying on implicts magic to make things work in Scala

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
  def docids(): DocidRDD = {
    new DocidRDD(sc.parallelize(0 to (numDocs() - 1) toList), path, numDocs)
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


/**
  * An RDD comprised of document ids and metadata about the underlying index.
  */
class DocidRDD(rdd: RDD[Int], path: String, numDocs: Int) extends RDD[Int](rdd) {

  override def compute(split: Partition, context: TaskContext): Iterator[Int] = 
    firstParent[Int].iterator(split, context)

  override protected def getPartitions: Array[Partition] = 
    firstParent[Int].partitions

  def indexPath(): String = path
  def numDocs(): Int = numDocs

  def docs[T: ClassTag](extractor: Document => T): RDD[T] = {
    rdd.mapPartitions(iter => {
      val reader = DirectoryReader.open(new HdfsReadOnlyDirectory(new Configuration(), new Path(path)))
      iter.map(doc => extractor(reader.document(doc)))
    })
  }

  // This is primarily for Python, since a Document isn't serializable.
  def docs(): JavaRDD[HashMap[String, String]] = docs(doc => {
    val map = new HashMap[String, String]()
    for (field <- doc.getFields.asScala) {
      map.put(field.name(), field.stringValue())
    }
    map
  }).toJavaRDD();

}