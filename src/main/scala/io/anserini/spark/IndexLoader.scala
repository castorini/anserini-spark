package io.anserini.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.lucene.document.Document
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexReader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import io.anserini.hadoop.HdfsReadOnlyDirectory

import scala.reflect.ClassTag

class IndexLoader(sc: SparkContext, path: String) {
  val conf = new Configuration()
  val reader = DirectoryReader.open(new HdfsReadOnlyDirectory(conf, new Path(path)))

  def numDocs() = {
    reader.numDocs()
  }

  def docids: RDD[Int] = {
    val arr = (0 to numDocs()-1).toArray
    sc.parallelize(arr)
  }
}
