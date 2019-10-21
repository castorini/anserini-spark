package io.anserini.spark

//import java.util.stream.{Collectors, IntStream}
//import java.util.{ArrayList, HashMap, Iterator}
import java.util.HashMap

import io.anserini.hadoop.HdfsReadOnlyDirectory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.lucene.document.Document
import org.apache.lucene.index.DirectoryReader
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
//import org.apache.spark.api.java.function.FlatMapFunction
//import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object IndexLoader {

//  def docs[T: ClassTag](rdd: RDD[Int], path: String, extractor: Document => T): RDD[T] = {
//    rdd.mapPartitions(iter => {
//      val reader = DirectoryReader.open(new HdfsReadOnlyDirectory(new Configuration(), new Path(path)))
//      iter.map(doc => extractor(reader.document(doc)))
//    })
//  }

  // Used in PySpark... need to return results as a HashMap since a Lucene's Document can't be serialized
  //def docs2map(rdd: RDD[Int], path: String): RDD[HashMap[String, String]] = docs(rdd, path, doc => {
  //  val map = new HashMap[String, String]()
  //  for (field <- doc.getFields.asScala) {
  //    map.put(field.name(), field.stringValue())
  //  }
  //  map
  //});

}

/**
  * Load document IDs from a Lucene index
  */
class IndexLoader(sc: SparkContext, path: String) {

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

  def docs(): RDD[HashMap[String, String]] = docs(doc => {
    val map = new HashMap[String, String]()
    for (field <- doc.getFields.asScala) {
      map.put(field.name(), field.stringValue())
    }
    map
  });

}