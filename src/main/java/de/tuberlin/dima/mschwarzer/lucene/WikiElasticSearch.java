package de.tuberlin.dima.mschwarzer.lucene;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.regex.Matcher;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class WikiElasticSearch {
    /**
     * Logger for this class.
     */
    private static Logger LOG = Logger.getLogger(WikiElasticSearch.class);

    private Client client;
    private String targetClusterName = "dima-power-cluster";
    private String targetIndex;
    private String targetType;
    private String targetHost;

    private long timer = 0;
    private int documentsSent = 0;

    private static final boolean NODE_CLIENT = false;
    private static final int REQUESTS_PER_LOG = 1000;
    private static final int REQUESTS_PER_BULK = 250;
    private static final int WIKI_VALID_NAMESPACE = 0;

    public final static String DEFAULT_INDEX = "wikipedia";
    public final static String DEFAULT_TYPE = "article";

    private BulkRequestBuilder bulkRequest;

    public static void main(String[] args) throws IOException, InterruptedException {
        if(args.length < 2) {
            System.err.println("Error - Arguments missing!");
            System.err.println(getParameters());
            System.exit(1);
        }

        new WikiElasticSearch(
                args[0], // hdfsPath
                args[1], // ES host
                (args.length >= 3 ? args[2] : DEFAULT_INDEX),
                (args.length >= 4 ? args[3] : DEFAULT_TYPE),
                (args.length >= 5 ? Integer.valueOf(args[4]) : 0),
                (args.length >= 6 ? Integer.valueOf(args[5]) : -1),
                (args.length >= 7 && args[6].equals("y") ? true : false)

        );
    }

    public Client getClient() {
        if(client == null) {
            if(NODE_CLIENT) {
                Node node = nodeBuilder().clusterName(targetClusterName).node();
                client = node.client();
            } else {
                Settings settings = ImmutableSettings.settingsBuilder()
                        .put("cluster.name", targetClusterName).build();

                client = new TransportClient(settings)
                        .addTransportAddress(new InetSocketTransportAddress(targetHost, 9300));
            }
        }

        return client;
    }

    public WikiElasticSearch(String hdfsPath, String host, String index, String type, int start, int limit, boolean reset) throws IOException, InterruptedException {
        targetIndex = index;
        targetType = type;
        targetHost = host;

        LOG.debug("Connected to ES: " + targetHost + "(" + targetClusterName + ")");

        if(reset) {
            deleteDocuments();
        }

        HDFSReader hdfs = new HDFSReader(hdfsPath);
        BufferedReader bfr = hdfs.getReader();

        int i = 0;
        String line = null;
        String content = "";

        bulkRequest = getClient().prepareBulk();

        // Read XML dump: Iterate lines
        while ((line = bfr.readLine()) != null) {

            // Look for delimiter
            int tagPosition = line.indexOf("</page>");

            if (tagPosition > -1) {

                content += line.substring(0, tagPosition);
                if(i >= start) {
                    // search for redirect -> skip if found
                    if (!WikiUtils.getRedirectMatcher(content).find()) {

                        // Parse page xml
                        Matcher m = WikiUtils.getPageMatcher(content);

                        // If found and namespace is valid
                        if (m.find() && Integer.parseInt(m.group(2)) == WIKI_VALID_NAMESPACE) {

                            // Add document and send bulk request
                            try {
                                addDocument(WikiUtils.unescapeEntities(m.group(1)), WikiUtils.unescapeEntities(m.group(4)), i);
                            } catch (Exception e) {
                                LOG.error("Failed adding document #" + i + "\n" + e.getMessage());
                            }

                            // Count documents
                            i++;

                            if ((i % REQUESTS_PER_LOG) == 0) {
                                LOG.warn("Importing articles ... " + i);
                            }

                            if (limit > 0 && i >= limit) {
                                LOG.warn("Limit reached (" + i + "/" + limit + ")");
                                break;
                            }
                        }
                    } else {
                        LOG.debug("Skip redirect");
                    }
                }
                content = line.substring(tagPosition) + "\n";

            } else {
                // Concatenate lines
                content += line + "\n";
            }
        }

        sendBulkRequest();
        hdfs.close();
    }

    public void addDocument(String title, String text, int i) throws Exception {
        // verify json string
        //String json = "{\"title\":\"" + WikiUtils.escapeQuotes(title) + "\",\"text\":\"" +  WikiUtils.escapeQuotes(text) + "\"}";

        LOG.debug("Adding doc: " + title);

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(new Document(title, text));

        IndexRequestBuilder request = getClient().prepareIndex(targetIndex, targetType)
                .setSource(json);

        bulkRequest.add(request);

        if((i % REQUESTS_PER_BULK) == 0) {
            sendBulkRequest();
        }

    }

    public void sendBulkRequest() {

        long timerStart = System.currentTimeMillis();

        if(bulkRequest.numberOfActions() < 1) {
            LOG.warn("Empty bulk request. Skip..");
            return;
        }

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        long timerEnd = System.currentTimeMillis();
        int timeReq = (int) (timerEnd - timerStart);
        double docsPerSec = REQUESTS_PER_BULK / timeReq * 1000;

        LOG.warn("BulkRequest took " + (timeReq) + "ms | " + docsPerSec +" docs/sec");

        if (bulkResponse.hasFailures()) {
            LOG.error("Error sending bulk request: " + bulkResponse.buildFailureMessage());
        }

        bulkRequest = getClient().prepareBulk();
    }

    public void deleteDocuments() {
        // delete all documents before importing ...

        LOG.warn("Deleting all docs from index ...");

        getClient().prepareDeleteByQuery(targetIndex).
                setQuery(QueryBuilders.matchAllQuery()).
                setTypes(targetType).
                execute().actionGet();

        LOG.warn("Docs deleted");
    }

    public static String getParameters() {
        return "Parameters: HDFS_PATH ES_NODE [ES_INDEX] [ES_TYPE] [START] [LIMIT] [RESET (y/n)]";
    }

    class Document {
        public String title;
        public String text;

        public Document(String title, String text) {
            this.title = title;
            this.text = text;
        }
    }
}
