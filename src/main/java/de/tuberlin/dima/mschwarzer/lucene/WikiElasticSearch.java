package de.tuberlin.dima.mschwarzer.lucene;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.regex.Matcher;

public class WikiElasticSearch {
    /**
     * Logger for this class.
     */
    private static Logger LOG = Logger.getLogger(WikiElasticSearch.class);

    Client client;
    String targetClusterName = "dima-power-cluster";
    String targetIndex;
    String targetType;
    String targetHost;

    public final static String DEFAULT_INDEX = "wikipedia";
    public final static String DEFAULT_TYPE = "article";

    public static void main(String[] args) throws IOException, InterruptedException {
        if(args.length < 2) {
            System.err.println("Error - Arguments missing!");
            System.err.println(getParameters());
            System.exit(1);
        }

        new WikiElasticSearch(
                args[0],
                args[1],
                (args.length >= 3 ? args[2] : DEFAULT_INDEX),
                (args.length >= 4 ? args[3] : DEFAULT_TYPE),
                (args.length >= 5 ? Integer.valueOf(args[4]) : 0),
                (args.length >= 6 ? Integer.valueOf(args[5]) : -1),
                (args.length >= 7 && args[6].equals("y") ? true : false)

        );
    }

    public WikiElasticSearch(String hdfsPath, String host, String index, String type, int start, int limit, boolean reset) throws IOException, InterruptedException {
        targetIndex = index;
        targetType = type;
        targetHost = host;

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", targetClusterName).build();

        client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(targetHost, 9300));

        LOG.debug("Connected to ES: " + targetHost + "(" + targetClusterName + ")");

        if(reset) {
            deleteDocuments();
        }

        HDFSReader hdfs = new HDFSReader(hdfsPath);
        BufferedReader bfr = hdfs.getReader();

        int i = 0;
        String line = null;
        String content = "";

        while ((line = bfr.readLine()) != null) {

            int tagPosition = line.indexOf("</page>");

            if (tagPosition > -1) {
                content += line.substring(0, tagPosition);
                if(i >= start) {
                    Matcher m = WikiUtils.getPageMatcher(content);

                    if (m.find() && Integer.parseInt(m.group(2)) == 0) {
                        // Add document
                        try {
                            addDocument(WikiUtils.unescapeEntities(m.group(1)), WikiUtils.unescapeEntities(m.group(4)), i);
                        } catch(Exception e) {
                            LOG.error("Failed adding document #" + i + "\n" + e.getMessage());
                        }

                        // Count documents
                        i++;

                        if((i%1000) == 0) {
                            LOG.debug("Importing articles ... " + i);
                        }

                        if(limit > 0 && i >= limit) {
                            LOG.warn("Limit reached (" + i + "/" + limit + ")");
                            break;
                        }
                    }
                }
                content = line.substring(tagPosition);

            } else {
                content += line;
            }
        }

        hdfs.close();
    }

    public void addDocument(String title, String text, int i) throws Exception {
        // verify json string
        //String json = "{\"title\":\"" + WikiUtils.escapeQuotes(title) + "\",\"text\":\"" +  WikiUtils.escapeQuotes(text) + "\"}";


        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(new Document(title, text));

        IndexResponse response = client.prepareIndex(targetIndex, targetType)
                .setSource(json)
                .setOperationThreaded(false)
                .execute()
                .actionGet();

        if (!response.isCreated()) {
            throw new Exception("Cannot create index for: " + json);
        }

    }

    public void deleteDocuments() {
        // delete all documents before importing ...

        LOG.debug("Deleting all docs from index ...");

        client.prepareDeleteByQuery(targetIndex).
                setQuery(QueryBuilders.matchAllQuery()).
                setTypes(targetType).
                execute().actionGet();

        LOG.debug("Docs deleted");
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
