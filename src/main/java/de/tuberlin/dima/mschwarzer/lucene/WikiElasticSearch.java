package de.tuberlin.dima.mschwarzer.lucene;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;

/**
 * Created by malteschwarzer on 30.11.14.
 */
public class WikiElasticSearch {
    Client client;
    String targetIndex;
    String targetType;

    public static void main(String[] args) {
        System.out.println("foo");
    }


    public WikiElasticSearch(String hdfsPath, String index, String type) throws IOException, InterruptedException {
        targetIndex = index;
        targetType = type;

        client = new TransportClient()
                .addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        HDFSReader hdfs = new HDFSReader(hdfsPath);
        BufferedReader bfr = hdfs.getReader();

        int i = 0;
        String line = null;
        String content = "";

        while ((line = bfr.readLine()) != null) {


            int tagPosition = line.indexOf("</page>");

            if (tagPosition > -1) {
                content += line.substring(0, tagPosition);
                Matcher m = WikiUtils.getPageMatcher(content);

                if(m.find() && Integer.parseInt(m.group(2)) == 0) {
                    addDocument(WikiUtils.unescapeEntities(m.group(1)), WikiUtils.unescapeEntities(m.group(4)));
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

        hdfs.close();
    }

    public void addDocument(String title, String text) {
        String json = "{title:\"\",text:\"\"}";

        IndexResponse response = client.prepareIndex(targetIndex, targetType)
                .setSource(json)
                .setOperationThreaded(false)
                .execute()
                .actionGet();

        if (!response.isCreated())
            System.err.println("Cannot create index for: " + json);
    }

}
