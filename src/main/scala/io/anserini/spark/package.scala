package io.anserini

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.lucene.document.Document
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexReader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import io.anserini.hadoop.HdfsReadOnlyDirectory

import scala.reflect.ClassTag

package object spark {
 implicit class DocidRDD[T: ClassTag](rdd: RDD[Int]) extends java.io.Serializable {
    def docs(path: String, extractor: Document => T): RDD[T] = {
      rdd.mapPartitions(iter => {
        val reader = DirectoryReader.open(new HdfsReadOnlyDirectory(new Configuration(), new Path(path)))
        iter.map(doc => extractor(reader.document(doc)))
      })
    }
  }
}