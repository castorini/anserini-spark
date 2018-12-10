package io.anserini.spark;

import io.anserini.hadoop.HdfsReadOnlyDirectory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.spark.api.java.JavaRDD;
import scala.reflect.ClassManifestFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.stream.StreamSupport;

public class JavaLuceneRDD extends JavaRDD<Integer> {

  public JavaLuceneRDD(JavaRDD<Integer> rdd) {
    super(JavaRDD.toRDD(rdd), ClassManifestFactory.fromClass(Integer.class));
  }

  public JavaRDD<HashMap<String, String>> getDocs(String path) {

    return wrapRDD(rdd()).mapPartitions((part) -> {

      // IndexReader per partition
      IndexReader reader = DirectoryReader.open(new HdfsReadOnlyDirectory(new Configuration(), new Path(path)));

      // Convert to Iterable
      Iterable<Integer> iterable = () -> part;

      return StreamSupport.stream(iterable.spliterator(), false).map(doc -> {

        HashMap<String, String> fields = new HashMap();

        try {

          Document document = reader.document(doc);

          for (IndexableField field : document.getFields()) {
            fields.put(field.name(), field.stringValue());
          }

        } catch (IOException e) {
          e.printStackTrace();
        }

        return fields;

      }).iterator();

    });

  }

}
