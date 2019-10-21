package io.anserini

import org.apache.lucene.document.Document
import org.apache.spark.SparkContext
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

package object spark {

  // Convert JavaRDD to RDD
  implicit def java2scala[T: ClassTag](rdd: JavaRDD[T]): RDD[T] = JavaRDD.toRDD(rdd)

  // Convert RDD to JavaRDD
  implicit def scala2java[T: ClassTag](rdd: RDD[T]): JavaRDD[T] = JavaRDD.fromRDD(rdd)

  // Convert JavaSparkContext to SparkContext
  implicit def java2scala(sc: JavaSparkContext): SparkContext = JavaSparkContext.toSparkContext(sc)

  // Convert SparkContext to JavaSparkContext
  implicit def scala2java(sc: SparkContext): JavaSparkContext = JavaSparkContext.fromSparkContext(sc)

  // Add docs(...) methods to JavaRDD[Integer] class.
  implicit class DocRDD[T: ClassTag](rdd: RDD[Int]) extends java.io.Serializable {
    def docs(path: String, extractor: Document => T): RDD[T] = IndexLoader.docs(rdd, path, extractor)
  }

}