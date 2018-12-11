package io.anserini.spark;

import io.anserini.hadoop.HdfsReadOnlyDirectory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Java version of the Scala IndexLoader that should play well with PySpark
 */
public class JavaIndexLoader {

  private final JavaSparkContext sparkContext;
  private final IndexReader reader;

  public JavaIndexLoader(JavaSparkContext sparkContext, String path) throws IOException {
    this.sparkContext = sparkContext;
    this.reader = DirectoryReader.open(new HdfsReadOnlyDirectory(new Configuration(), new Path(path)));
  }

  public int numDocs() {
    return reader.numDocs();
  }

  /**
   * Get the document IDs as an RDD
   *
   * @return an RDD of document IDs
   */
  public JavaRDD<Integer> docIds() {
    return docIds(numDocs() - 1);
  }

  /**
   * Get the document IDs as an RDD
   *
   * @param num the number of documents
   * @return an RDD of document IDs
   */
  public JavaRDD<Integer> docIds(int num) {
    return sparkContext.parallelize(docIdsAsList(num));
  }

  /**
   * Get the document IDs as a List
   *
   * @param n the number of documents
   * @return a List of document IDs
   */
  private List<Integer> docIdsAsList(int n) {
    return IntStream.rangeClosed(0, n).boxed().collect(Collectors.toList());
  }

}