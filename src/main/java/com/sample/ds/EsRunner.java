package com.sample.ds;


import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;

import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.search.SearchHit;
import org.xbib.elasticsearch.river.jdbc.JDBCRiver;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverFlow;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverMouth;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverSource;
import org.xbib.elasticsearch.river.jdbc.support.SQLCommand;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import org.xbib.elasticsearch.river.jdbc.support.RiverKeyValueStreamListener;
import org.xbib.io.keyvalue.KeyValueStreamListener;

import antlr.collections.List;



public class EsRunner {
	private static final ESLogger logger = Loggers.getLogger(EsRunner.class.getSimpleName());
	public final String INDEX = "my_jdbc_river_index";

    public final String TYPE = "my_jdbc_river_type";

	protected static RiverSource source;

    protected static RiverContext context;
    
    private Map<String, Client> clients = newHashMap();
    
    private Map<String, Node> nodes = newHashMap();
    
    private final static AtomicLong counter = new AtomicLong();
    
    private String resource = "/Users/vera.kalinichenko/data/datascience/pig-template/src/main/pig/correctedRtp/config.json";
    private String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private String user = "dev-pm";
    String url = "jdbc:sqlserver://10.0.5.60:1433;databaseName=Interactive-Logging";
    String url1 = "jdbc:sqlserver://10.0.5.60:1433;databaseName=Interactive-PatronManager";
    
    private String password = "gwpm%6234";
    String storedProcSQL = "Exec App.GETAllWagers";
    
    
    public EsRunner()
    {
    	try {
    		createClient(driver, url, user, password);
    	}
    	catch (Exception exc) {
    		System.out.println("");
    	}
    }
    public RiverSource getRiverSource() {
        return new SimpleRiverSource();
    }
   
    public RiverContext getRiverContext() {
    	if (context == null){
    		context = new RiverContext();
    		
    	}
    	return context;
    }

    public void createClient(String driver, String starturl, String user, String password)
            throws Exception {
        source = getRiverSource()
                .url(starturl)
                .user(user)
                .password(password);
        context = getRiverContext()
                .riverSource(source)
                .setRetries(1)
                .setMaxRetryWait(TimeValue.timeValueSeconds(5))
                .setLocale("en");
        context.contextualize();
        ArrayList<SQLCommand> sql = new ArrayList<SQLCommand>();
        SQLCommand command = new SQLCommand();
        command.setCallable(true);
        command.setSQL(storedProcSQL);
        sql.add(command);
        context.setStatements(sql);
        createIndices();
    }
   
    private Settings defaultSettings() {
        return ImmutableSettings
            .settingsBuilder()
            .put("cluster.name", "testing-jdbc-river-on-" + NetworkUtils.getLocalAddress().getHostName() + "-" + counter.incrementAndGet())
            .build();
    }
    public static void main(String args[]) throws IOException{

//        Node node     = nodeBuilder().node();
//        Client client = node.client();
//       
//        /**
//         * it's very important to know your cluster-name and refer to it
//		 *	curl -XGET 'http://localhost:9200/_cluster/nodes?pretty=true'
//         */
//        Settings settings = ImmutableSettings.settingsBuilder()
//
//                .put("cluster.name", "elasticsearch").build();
//
//        Client clientTester = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
//        Client cl = new TransportClient(settings)
//        		.addTransportAddress(new
//        		InetSocketTransportAddress("localhost", 9300));
//        		
//        SearchResponse response =
//
//        		cl.prepareSearch("outcome-ch12").setSearchType(
//        		SearchType.COUNT).setQuery(QueryBuilders.termQuery("reel1",
//
//        		"cherry")).setFrom(0).setSize(60).setExplain(true).execute().actionGet();
//        
//        // more samples
//        QueryBuilder queryBuilder = QueryBuilders.termQuery("reel2", "seven");
//        SearchRequestBuilder searchRequestBuilder = cl.prepareSearch("outcome-1gb");
//        searchRequestBuilder.setTypes("all");
//        searchRequestBuilder.setSearchType(SearchType.COUNT);
//        searchRequestBuilder.setQuery(queryBuilder);
//        searchRequestBuilder.setFrom(0).setSize(60).setExplain(true);
//        SearchResponse resp = searchRequestBuilder.execute().actionGet();
//        System.out.println(resp);
//        
//        for (SearchHit hit : resp.getHits()) {
//          System.out.println("Hit ID: "+hit.getId());
//          
//        }
//
//        		SearchHit[] docs = response.getHits().getHits();
//        		System.out.println("**** = "  + response);
//        		for (int i=0; i<docs.length; ++i) {
//        			System.out.println("results .. doc[" + i + "] = " + docs[i].sourceAsString());
//        			Map map = docs[i].sourceAsMap();
//        			Iterator iter = map.values().iterator();
//        			while (iter.hasNext()) {
//        				Object current = iter.next();
//        				System.out.print(" key  = " + current + "\n");
//        			}
//        		}
//        
//        /**
//         * just more samples
//         */
//        client.prepareIndex("sample", "article", "1")
//              .setSource(putJsonDocument("ElasticSearch: Java",
//                                         "ElasticSeach provides Java API, thus it executes all operations " +
//                                          "asynchronously by using client object..",
//                                         new Date(),
//                                         new String[]{"elasticsearch"},
//                                         "Hüseyin Akdoğan")).execute().actionGet();
//        
//        client.prepareIndex("sample", "article", "2")
//              .setSource(putJsonDocument("Java Web Application and ElasticSearch (Video)",
//                                         "Today, here I am for exemplifying the usage of ElasticSearch which is an open source, distributed " +
//                                         "and scalable full text search engine and a data analysis tool in a Java web application.",
//                                         new Date(),
//                                         new String[]{"elasticsearch"},
//                                         "Hüseyin Akdoğan")).execute().actionGet();
//        
//        getDocument(client, "sample", "article", "1");
//
//        updateDocument(client, "sample", "article", "1", "title", "ElasticSearch: Java API");
//        updateDocument(client, "sample", "article", "1", "tags", new String[]{"bigdata"});
//
//        getDocument(client, "sample", "article", "1");
//
//        searchDocument(client, "sample", "article", "title", "ElasticSearch");
//        
//        deleteDocument(client, "sample", "article", "1");
//        
//        node.close();
    	EsRunner myRunner = new EsRunner();
    	try {
    		myRunner.testVeraStoredProcedure();
    	}
    	catch (Exception exc) 
    	{
    		logger.info("error" + exc.getMessage());
    	}
    	
    }
    
    public static Map<String, Object> putJsonDocument(String title, String content, Date postDate, 
                                                      String[] tags, String author){
        
        Map<String, Object> jsonDocument = new HashMap<String, Object>();
        
        jsonDocument.put("title", title);
        jsonDocument.put("content", content);
        jsonDocument.put("postDate", postDate);
        jsonDocument.put("tags", tags);
        jsonDocument.put("author", author);
        
        return jsonDocument;
    }
    
    public static void getDocument(Client client, String index, String type, String id){
        
        GetResponse getResponse = client.prepareGet(index, type, id)
                                        .execute()
                                        .actionGet();
        Map<String, Object> source = getResponse.getSource();
        
        System.out.println("------------------------------");
        System.out.println("Index: " + getResponse.getIndex());
        System.out.println("Type: " + getResponse.getType());
        System.out.println("Id: " + getResponse.getId());
        System.out.println("Version: " + getResponse.getVersion());
        System.out.println(source);
        System.out.println("------------------------------");
        
    }
    
    public static void updateDocument(Client client, String index, String type, 
                                      String id, String field, String newValue){
        
        Map<String, Object> updateObject = new HashMap<String, Object>();
        updateObject.put(field, newValue);
        
        client.prepareUpdate(index, type, id)
              .setScript("ctx._source." + field + "=" + field)
              .setScriptParams(updateObject).execute().actionGet();
    }

    public static void updateDocument(Client client, String index, String type,
                                      String id, String field, String[] newValue){

        String tags = "";
        for(String tag :newValue)
            tags += tag + ", ";

        tags = tags.substring(0, tags.length() - 2);

        Map<String, Object> updateObject = new HashMap<String, Object>();
        updateObject.put(field, tags);

        client.prepareUpdate(index, type, id)
                .setScript("ctx._source." + field + "+=" + field)
                .setScriptParams(updateObject).execute().actionGet();
    }

    public static void searchDocument(Client client, String index, String type,
                                      String field, String value){
        
        SearchResponse response = client.prepareSearch(index)
                                        .setTypes(type)
                                        .setSearchType(SearchType.QUERY_AND_FETCH)
                                        .setQuery("*")
                                        .setFrom(0).setSize(60).setExplain(true)
                                        .execute()
                                        .actionGet();
        
        SearchHit[] results = response.getHits().getHits();
        
        System.out.println("Current results: " + results.length);
        for (SearchHit hit : results) {
            System.out.println("------------------------------");
            Map<String,Object> result = hit.getSource();   
            System.out.println(result);
        }
    }
    
    public static void deleteDocument(Client client, String index, String type, String id){
        
        DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();
        System.out.println("Information on the deleted document:");
        System.out.println("Index: " + response.getIndex());
        System.out.println("Type: " + response.getType());
        System.out.println("Id: " + response.getId());
        System.out.println("Version: " + response.getVersion());
    }
    protected RiverSettings riverSettings(String resource)
            throws IOException {
        InputStream in = getClass().getResourceAsStream(resource);
        return new RiverSettings(ImmutableSettings.settingsBuilder()
                .build(), XContentHelper.convertToMap(Streams.copyToByteArray(in), false).v2());
    }
    public void testVeraStoredProcedure()
            throws Exception {
    	System.err.println("************************************* Hello !!!!!!!!!!!!");
    	String riverResource = resource;
    	
    	
//        Connection connection = source.connectionForWriting();
        System.err.println("*************************************");
        // create stored procedure
//        Statement statement = connection.createStatement();
//        statement.execute(storedProcSQL);
//        ResultSet result = statement.getResultSet();
//        while(result.next())
//        {
//        	int playerId = result.getInt("WagerID");
//        	String wagerOut = result.getString("Outcome");
//        	System.out.println("playerId = " + playerId + " outcome = " + wagerOut);
//        }
        Connection con =  source.connectionForReading();
        source.riverContext(getRiverContext());
        source.fetch();
        
       
        
//        createClient(driver, url1, user, password);
//        Connection connection1 = source.connectionForWriting();
//        
//        Statement my = connection1.createStatement();
////        result = my.executeQuery("select * from AuthorizedPlayer.PlayerInfo");
//        
//        result = my.executeQuery("Exec AuthorizedPlayer.GetMultiplePlayerInfo @Offset=0, @Max_value=100");
//            
//        	while(result.next())
//        	{
//	        	int playerId = result.getInt("PlayerID");
//	        	String ans = result.getString("ChallengeAnswer1");
//	        	System.out.println("playerId = " + playerId + " outcome = " + ans);
//        	}
//        
//        SimpleRiverFlow flow = new SimpleRiverFlow().riverContext(context);
//        SimpleRiverMouth mouth = new SimpleRiverMouth().riverContext(context);
//        buildNode("sample", defaultSettings());
//        Client client = client("1");
//        mouth.client(client);
//        
//        KeyValueStreamListener listener = new RiverKeyValueStreamListener()
//                .output(mouth);
//        long rows = 0L;
//        source.beforeRows(result, listener);
//        if (source.nextRow(result, listener)) {
//            // only one row
//            rows++;
//        }
//        source.afterRows(result, listener);
//        
//        mouth.waitForCluster();
//        flow.strategy();
//        flow.move();
//        statement.close();

        
//        Client client2 = client("2");
//        RiverSettings settings = riverSettings(riverResource);
//        JDBCRiver river = new JDBCRiver(new RiverName(INDEX, TYPE), settings, client2);
//        river.start();
//        Thread.sleep(10000L); // let the river run
//        System.err.println("Try out out client result");
//        System.err.println(client2.prepareSearch(INDEX).execute().actionGet().getHits().getTotalHits());
//        source.closeWriting();
//        river.close();
    }
    public void createIndices() throws Exception {
        startNode("1").client();
        client("1").admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        client("1").admin().indices().create(new CreateIndexRequest(INDEX)).actionGet();
    }

   
    public void deleteIndices() {
        try {
            // clear test index
            client("1").admin().indices()
                    .delete(new DeleteIndexRequest().indices(INDEX))
                    .actionGet();
        } catch (IndexMissingException e) {
            logger.error(e.getMessage());
        }
        try {
            // clear rivers
            client("1").admin().indices()
                    .delete(new DeleteIndexRequest().indices("_river"))
                    .actionGet();
        } catch (IndexMissingException e) {
            logger.error(e.getMessage());
        }
        closeNode("1");
        closeAllNodes();
    }


    public Node startNode(String id) {
        return buildNode(id).start();
    }

    public Node buildNode(String id) {
        return buildNode(id, defaultSettings());
    }

    public Node buildNode(String id, Settings settings) {
        String settingsSource = getClass().getName().replace('.', '/') + ".yml";
        Settings finalSettings = settingsBuilder()
                .loadFromClasspath(settingsSource)
                .put(settings)
                .put("name", id)
                .build();

        if (finalSettings.get("gateway.type") == null) {
            // default to non gateway
            finalSettings = settingsBuilder().put(finalSettings).put("gateway.type", "none").build();
        }

        Node node = nodeBuilder()
                .settings(finalSettings)
                .build();
        nodes.put(id, node);
        clients.put(id, node.client());
        return node;
    }

    public void closeNode(String id) {
        Client client = clients.remove(id);
        if (client != null) {
            client.close();
        }
        Node node = nodes.remove(id);
        if (node != null) {
            node.close();
        }
    }

    public Client client(String id) {
        return clients.get(id);
    }

    public void closeAllNodes() {
        for (Client client : clients.values()) {
            client.close();
        }
        clients.clear();
        for (Node node : nodes.values()) {
            node.close();
        }
        nodes.clear();
    }
}
