package io.anserini.spark

import io.anserini.hadoop.HdfsReadOnlyDirectory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.lucene.index.DirectoryReader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class IndexLoader(sc: SparkContext, path: String) {
  val conf = new Configuration()
  val reader = DirectoryReader.open(new HdfsReadOnlyDirectory(conf, new Path(path)))

  def docids: RDD[Int] = {
    val arr = (0 to numDocs() - 1).toArray
    sc.parallelize(arr)
  }

  def numDocs() = {
    reader.numDocs()
  }

  def docidsN(n: Int): RDD[Int] = {
    val arr = (0 to n - 1).toArray
    sc.parallelize(arr)
  }
}
