package de.tuberlin.dima.mschwarzer.lucene;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.commons.lang3.text.translate.CharSequenceTranslator;
import org.apache.commons.lang3.text.translate.EntityArrays;
import org.apache.commons.lang3.text.translate.LookupTranslator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by malteschwarzer on 17.11.14.
 */
public class WikiLucene {

    public static void main(String[] args) throws IOException, Exception {

        importWikiXml(
                (args.length > 0 ? args[0] : "hdfs://ibm-power-1.dima.tu-berlin.de:8020/user/mschwarzer/inputtest"),
                (args.length > 1 ? args[1] : "/home/mschwarzer/lucene/")
        );
    }

    public static void importWikiXml(String hdfsPath, String indexPath) throws IOException, InterruptedException {

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://ibm-power-1.dima.tu-berlin.de:8020/");
        conf.set("fs.default.name", "hdfs://ibm-power-1.dima.tu-berlin.de:8020/");

        conf.set("io.file.buffer.size", "131072");
        conf.set("hadoop.http.staticuser.user", "hadoop");
        conf.set("dfs.namenode.hosts", "ibm-power-1.dima.tu-berlin.de");
        conf.set("dfs.namenode.name.dir", "file:///hadoop/hdfs/hadoop/namenode");
        conf.set("dfs.datanode.data.dir", "file:///hadoop/hdfs/data/1/hadoop/datanode,file:///hadoop/hdfs/data/2/hadoop/datanode,file:///hadoop/hdfs/data/3/hadoop/datanode,file:///hadoop/hdfs/data/4/hadoop/datanode");
        conf.set("dfs.blocksize", "128m");

        // Init HDFS
        URI uri = URI.create(hdfsPath);
        FileSystem fileSystem = FileSystem.get(uri, conf, "hadoop");
        FSDataInputStream in = fileSystem.open(new Path(uri));
        BufferedReader bfr = new BufferedReader(new InputStreamReader(in));

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
                Matcher m = getPageMatcher(content);

                if(m.find() && Integer.parseInt(m.group(2)) == 0) {
                    addDoc(writer, unescapeEntities(m.group(1)), unescapeEntities(m.group(4)));
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
        fileSystem.close();

    }

    public static Matcher getPageMatcher(String content) {
        // TODO skip if <redirect> exists?

        // search for a page-xml entity
        Pattern pageRegex = Pattern.compile("(?:<page>\\s+)(?:<title>)(.*?)(?:</title>)\\s+(?:<ns>)(.*?)(?:</ns>)\\s+(?:<id>)(.*?)(?:</id>)(?:.*?)(?:<text.*?>)(.*?)(?:</text>)", Pattern.DOTALL);
        return pageRegex.matcher(content);
    }

    public static void addDoc(IndexWriter w, String title, String content) throws IOException {
        Document doc = new Document();

        doc.add(new TextField("title", title, Field.Store.YES));
        doc.add(new TextField("content", content, Field.Store.YES));

        System.out.println(title + "; " + content.length());

        w.addDocument(doc);
    }

    /**
     * Unescapes special entity char sequences like &lt; to its UTF-8 representation.
     * All ISO-8859-1, HTML4 and Basic entities will be translated.
     *
     * @param text the text that will be unescaped
     * @return the unescaped version of the string text
     */
    public static String unescapeEntities(String text) {
        CharSequenceTranslator iso = new LookupTranslator(EntityArrays.ISO8859_1_UNESCAPE());
        CharSequenceTranslator basic = new LookupTranslator(EntityArrays.BASIC_UNESCAPE());
        CharSequenceTranslator html4 = new LookupTranslator(EntityArrays.HTML40_EXTENDED_UNESCAPE());
        return html4.translate(iso.translate(basic.translate(text)));
    }
}
