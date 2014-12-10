package de.tuberlin.dima.mschwarzer.lucene.morelikethis;

import org.apache.log4j.Logger;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Collecting similar documents with MoreLikeThis for input articles
 *
 * Input format:
 * article
 *
 * Output format:
 * article | mlt result title | mlt result score
 *
 */
public class ResultCollector {
    private static char listDelimitter = ',';
    private static char csvFieldDelimitter = '|';
    private static String csvRowDelimitter = "\n";

    private static Logger LOG = Logger.getLogger(ResultCollector.class);
    private Client client;
    private String targetClusterName = "dima-power-cluster";
    private String targetIndex;
    private String targetType;
    private String targetHost;
    private int resultSize = 10;
    private static final boolean NODE_CLIENT = false;

    public class MLTResult {
        public String title;
        public float score;

        public MLTResult(String title, float score) {
            this.title = title;
            this.score = score;
        }
    }

    public static void main(String[] args) {

    }

    public void execute(String inputFilename, String outputFilename) throws IOException {

        String articleName = null;
        FileWriter writer = new FileWriter(outputFilename);
        BufferedReader bfr = new BufferedReader(new FileReader(inputFilename));

        LOG.warn("Reading from " + inputFilename);

        while ((articleName = bfr.readLine()) != null) {

            String docId = getDocumentId(articleName);

            if(docId == null) {
                LOG.error("No document matching article name: " + articleName);
            } else {

                for(MLTResult result : getMoreLikeThisResults(docId)) {
                    writer.append(articleName + csvFieldDelimitter);
                    writer.append(result.title + csvFieldDelimitter);
                    writer.append(result.score + csvRowDelimitter);
                }

            }

            writer.flush();
        }

        writer.close();
    }

    private String getDocumentId(String title) {
        SearchResponse response = getClient().prepareSearch(targetIndex)
                .setTypes(targetType)
                .setQuery(QueryBuilders.matchQuery("title", title))
                .setSize(1)
                .execute()
                .actionGet();

        String docId = null;

        for (SearchHit hit: response.getHits()) {
            docId = hit.getId();
        }

        return docId;
    }

    private List<MLTResult> getMoreLikeThisResults(String docId) {
        LOG.debug("Getting results for: " + docId);

        SearchResponse mlt = getClient()
            .moreLikeThis(new MoreLikeThisRequest(targetIndex)
                            .type(targetType)
                            .id(docId)
                            .fields("title", "text")
                            .minTermFreq(1)
                            .minDocFreq(1)
                            .searchSize(resultSize)
            )
            .actionGet();

        ArrayList<MLTResult> results = new ArrayList<MLTResult>();

        for (SearchHit hit : mlt.getHits()) {
            Map<String, Object> source = hit.getSource();
            results.add(new MLTResult((String) source.get("title"), hit.getScore()));
        }

        return results;
    }


    public Client getClient() {
        if(client == null) {
            if(NODE_CLIENT) {
                Node node = nodeBuilder().clusterName(targetClusterName).node();
                client = node.client();
            } else {
                ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();

                if(targetClusterName != null) {
                    settingsBuilder.put("cluster.name", targetClusterName);
                }

                client = new TransportClient(settingsBuilder.build())
                        .addTransportAddress(new InetSocketTransportAddress(targetHost, 9300));
            }
        }

        return client;
    }

    public ResultCollector setIndex(String index) {
        targetIndex = index;
        return this;
    }

    public ResultCollector setType(String type) {
        targetType = type;
        return this;
    }

    public ResultCollector setHost(String host) {
        targetHost = host;
        return this;
    }
    public ResultCollector setClusterName(String clusterName) {
        targetClusterName = clusterName;
        return this;
    }
}
