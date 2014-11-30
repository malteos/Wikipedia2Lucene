package de.tuberlin.dima.mschwarzer.lucene;

import org.apache.commons.lang3.text.translate.CharSequenceTranslator;
import org.apache.commons.lang3.text.translate.EntityArrays;
import org.apache.commons.lang3.text.translate.LookupTranslator;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Import Wikipedia dump from HDFS to Lucene index
 *
 */
public class WikiLucene {
    public final static String DEFAULT_WIKI_PATH = "hdfs://ibm-power-1.dima.tu-berlin.de:8020/user/mschwarzer/inputtest";
    public final static String DEFAULT_LUCENE_PATH =  "/home/mschwarzer/lucene/";

    public static void main(String[] args) throws IOException, Exception {

        importWikiXml(
                (args.length > 0 ? args[0] : DEFAULT_WIKI_PATH),
                (args.length > 1 ? args[1] : DEFAULT_LUCENE_PATH)
        );
    }

    public static void importWikiXml(String hdfsPath, String indexPath) throws IOException, InterruptedException {

        // Init HDFS
        HDFSReader hdfs = new HDFSReader(hdfsPath);
        BufferedReader bfr = hdfs.getReader();

        // Init Lucene
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);
        FSDirectory dir = FSDirectory.open(new File(indexPath));
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_40, analyzer);
        IndexWriter writer = new IndexWriter(dir, config);


        int i = 0;
        String line = null;
        String content = "";

        while ((line = bfr.readLine()) != null) {


            int tagPosition = line.indexOf("</page>");

            if (tagPosition > -1) {
                content += line.substring(0, tagPosition);
                Matcher m = WikiUtils.getPageMatcher(content);

                if(m.find() && Integer.parseInt(m.group(2)) == 0) {
                    addDoc(writer, WikiUtils.unescapeEntities(m.group(1)), WikiUtils.unescapeEntities(m.group(4)));
                }
                content = line.substring(tagPosition);

            } else {
                content += line;
            }
            //System.out.println(str);


            i++;

            if(i > 1000)
                break;
        }

        writer.close();
        hdfs.close();

    }



    public static void addDoc(IndexWriter w, String title, String content) throws IOException {
        Document doc = new Document();

        doc.add(new TextField("title", title, Field.Store.YES));
        doc.add(new TextField("content", content, Field.Store.YES));

        System.out.println(title + "; " + content.length());

        w.addDocument(doc);
    }
}
