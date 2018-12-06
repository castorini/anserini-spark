package io.anserini

import io.anserini.hadoop.HdfsReadOnlyDirectory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.lucene.document.Document
import org.apache.lucene.index.DirectoryReader
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

package object spark {

  implicit class DocidRDD[T: ClassTag](rdd: RDD[Int]) extends java.io.Serializable {
    def getDocs(path: String, extractor: Document => T): RDD[T] = {
      rdd.mapPartitions(iter => {
        val reader = DirectoryReader.open(new HdfsReadOnlyDirectory(new Configuration(), new Path(path)))
        iter.map(doc => extractor(reader.document(doc)))
      })
    }

    def getDocs(path: String): RDD[String] = {
      rdd.mapPartitions(iter => {
        val reader = DirectoryReader.open(new HdfsReadOnlyDirectory(new Configuration(), new Path(path)))
        iter.map(doc => reader.document(doc).toString)
      })
    }
  }

}