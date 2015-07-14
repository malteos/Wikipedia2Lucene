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
    private String outputFilename = null;
    private String inputFilename = null;
    private FileWriter outputFileWriter = null;
    private int scrollStart = 0;
    private int scrollEnd = -1;

    private int resultSize = 20;
    private static final boolean NODE_CLIENT = false;

    public class MLTResult {
        public String title;
        public float score;

        public MLTResult(String title, float score) {
            this.title = title;
            this.score = score;
        }
    }

    public static void main(String[] args) throws IOException {
        if(args.length < 5) {
            System.err.println("Arguments missing: <clustername> <host> <index> <type> <outputfile> <docfilter> <scroll-start> <scroll-limit>");
            System.exit(1);
        }

        ResultCollector rc = new ResultCollector()
                .setClusterName(args[0])
                .setHost(args[1])
                .setIndex(args[2])
                .setType(args[3])
                .setOutputFilename(args[4]);



        if(args.length > 5)
            rc.setInputFilename(args[5]);

        if(args.length > 6)
            rc.setScrollStart(Integer.valueOf(args[6]));

        if(args.length > 7)
            rc.setScrollEnd(Integer.valueOf(args[7]));

        rc.execute();
    }


    public void execute() throws IOException {

        LOG.warn("Executing...");

        if(inputFilename != null) {
            // Documents from input
            String articleName = null;
            BufferedReader bfr = new BufferedReader(new FileReader(inputFilename));

            LOG.warn("Reading from " + inputFilename);

            while ((articleName = bfr.readLine()) != null) {
                String docId = getDocumentId(articleName);

                if (docId == null) {
                    LOG.error("No document matching article name: " + articleName);
                    continue;
                }
                collectResults(articleName, docId);
            }
        } else {
            // All documents
            int scrollSize = 100;
            SearchResponse response = null;
            int scroll = 0;
            while (response == null || response.getHits().hits().length != 0) {
                if(scroll >= scrollStart) {
                    response = getClient().prepareSearch(targetIndex)
                            .setTypes(targetType)
                            .setQuery(QueryBuilders.matchAllQuery())
                            .setSize(scrollSize)
                            .setFrom(scroll * scrollSize)
                            .addFields("title")
                            .execute()
                            .actionGet();
                    for (SearchHit hit : response.getHits()) {
                        collectResults((String) hit.field("title").getValue(), hit.getId());
                    }
                    LOG.warn("Search scroll #" + scroll + " with " + response.getHits().hits().length + " hits");
                } else {
                    LOG.debug("Skip search scroll #" + scroll + " until #" + scrollStart);
                }
                scroll++;

                if(scrollEnd > 0 && scroll >= scrollEnd) {
                    LOG.warn("Scroll end reached at #" + scrollEnd);
                    break;
                }
            }
        }


        getOutputFileWriter().close();
    }

    public void collectResults(String articleName, String docId) throws IOException {

        for(MLTResult result : getMoreLikeThisResults(docId)) {
            getOutputFileWriter().append(articleName + csvFieldDelimitter);
            getOutputFileWriter().append(result.title + csvFieldDelimitter);
            getOutputFileWriter().append(result.score + csvRowDelimitter);
        }

        getOutputFileWriter().flush();
    }

    public FileWriter getOutputFileWriter() throws IOException {
        if(outputFileWriter == null) {
            outputFileWriter = new FileWriter(outputFilename);
        }

        return outputFileWriter;
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
                            .fields("text") // ignore title field?
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

    public ResultCollector setInputFilename(String filename) {
        if(!filename.equalsIgnoreCase("nofilter"))
            inputFilename = filename;

        return this;
    }

    public ResultCollector setOutputFilename(String filename) {
        outputFilename = filename;
        return this;
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

    public ResultCollector setScrollStart(int start) {
        scrollStart = start;
        return this;
    }

    public ResultCollector setScrollEnd(int end) {
        scrollEnd = end;
        return this;
    }
}
