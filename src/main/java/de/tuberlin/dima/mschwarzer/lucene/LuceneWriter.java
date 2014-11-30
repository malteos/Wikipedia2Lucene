package de.tuberlin.dima.mschwarzer.lucene;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;

/**
 * Created by malteschwarzer on 18.11.14.
 */
public class LuceneWriter {

    public static void main(String[] args) throws IOException, Exception {

        String indexDir = (args.length > 0 ? args[0] : "/home/mschwarzer/lucene/");
        String title = (args.length > 1 ? args[1] : "fooo");
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);


        FSDirectory dir = FSDirectory.open(new File(indexDir));

        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_40, analyzer);

        IndexWriter writer = new IndexWriter(dir, config);

        WikiLucene.addDoc(writer, title, title + title + title);

        writer.close();

    }
}
