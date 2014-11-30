package de.tuberlin.dima.mschwarzer.lucene;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;

/**
 * Created by malteschwarzer on 18.11.14.
 */
public class LuceneSearch {
    public static void main(String[] args) throws IOException, Exception {

        search(
                (args.length > 0 ? args[0] : "/home/mschwarzer/lucene/"),
                (args.length > 1 ? args[1] : "foo")
        );
    }

    public static void search(String indexDir, String querystr) throws IOException, ParseException {

        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);

        FSDirectory dir = FSDirectory.open(new File(indexDir));
        IndexReader reader = DirectoryReader.open(dir);

        // the "title" arg specifies the default field to use
        // when no field is explicitly specified in the query.
        Query q = new QueryParser(Version.LUCENE_40, "title", analyzer).parse(querystr);

        // 3. search
        int hitsPerPage = 10;
        IndexSearcher searcher = new IndexSearcher(reader);
        TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);
        searcher.search(q, collector);
        ScoreDoc[] hits = collector.topDocs().scoreDocs;

        // 4. display results
        System.out.println("Found " + hits.length + " hits.");
        for (int i = 0; i < hits.length; ++i) {
            int docId = hits[i].doc;
            Document d = searcher.doc(docId);
            System.out.println((i + 1) + ". "+ d.get("title") + " ["+ d.get("content").length() +"]");
        }

        // reader can only be closed when there
        // is no need to access the documents any more.
        reader.close();
    }
}
