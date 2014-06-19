package com.logging;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

/**
 * this class is based on the 
 * https://github.com/Downfy/log4j-elasticsearch-java-api
 * It has the Apache License
 * 
 */

public class ESCustomAppender  extends AppenderSkeleton{
    private ExecutorService threadPool = Executors.newSingleThreadExecutor();
    private JestClient client;    
    private String hostName = "local";
    private String index;
    private String type;
    private String app;
    private String esServer;
    private String message;
    private boolean reels;   
    

    private String r0;
    private String r1;
    private String r2;
    private String r3;
    private String r4;    
    private Date dt;
    private Long myTime;

    @Override
    protected void append(LoggingEvent loggingEvent) {
        if (isAsSevereAsThreshold(loggingEvent.getLevel())) {
            threadPool.submit(new AppenderTask(loggingEvent));
        }
    }

    /**
     * Create ElasticSearch Client.
     *
     * @see AppenderSkeleton
     */
    @Override
    public void activateOptions() {
        // Need to do this if the cluster name is changed, probably need to set this and sniff the cluster
        try {
            // Configuration
            // Construct a new Jest client according to configuration via factory
            JestClientFactory factory = new JestClientFactory();
            HttpClientConfig config = new HttpClientConfig.Builder(getEsServer()).multiThreaded(true).build();
            // Construct a new Jest client according to configuration via factory            
            factory.setHttpClientConfig(config);
            client = factory.getObject();
        } catch (Exception ex) {
        }

        super.activateOptions();
    }

    /**
     * Elastic Search host.
     *
     * @return
     */
    public String getEsServer() {
        return esServer;
    }

    /**
     * Elastic Search host.
     *
     * @param elasticHost
     */
    public void setEsServer(String elasticHost) {
        this.esServer = elasticHost;
    }

    /**
     * Elastic Search index.
     *
     * @return
     */
    public String getIndex() {
        return index;
    }

    /**
     * Elastic Search index.
     *
     * @param elasticIndex
     */
    public void setIndex(String elasticIndex) {
        this.index = elasticIndex;
    }

    /**
     * Elastic Search type.
     *
     * @return Type
     */
    public String getType() {
        return type;
    }

    /**
     * Elastic Search type.
     *
     * @param elasticType
     */
    public void setType(String elasticType) {
        this.type = elasticType;
    }

    /**
     * Name application using log4j.
     *
     * @return
     */
    public String getApp() {
        return app;
    }

    /**
     * Name application using log4j.
     *
     * @param applicationId
     */
    public void setApp(String applicationName) {
        this.app = applicationName;
    }

    /**
     * Host name application run.
     *
     * @return
     */
    public String getHostName() {
        return hostName;
    }

    /**
     * Host name application run.
     *
     * @param ip
     */
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    /**
     * Close Elastic Search client.
     */
    
    public void close() {
        client.shutdownClient();
    }

    /**
     * Ensures that a Layout property is not required
     *
     * @return
     */
    
    public boolean requiresLayout() {
        return false;
    }

    /**
     * Simple Callable class that insert the document into ElasticSearch
     */
    class AppenderTask implements Callable<LoggingEvent> {

        LoggingEvent loggingEvent;

        AppenderTask(LoggingEvent loggingEvent) {
            this.loggingEvent = loggingEvent;
        }

        protected void writeBasic(Map<String, Object> json, LoggingEvent event) {
            json.put("hostName", getHostName());
            json.put("applicationName", getApp());
            json.put("timestamp", new Date(event.getTimeStamp()));
            json.put("logger", event.getLoggerName());
            json.put("level", event.getLevel().toString());
            json.put("myTimeES", new Date(event.getTimeStamp()).getTime());
            if(getReels())
            {
            	enchanceMapData(json, event.getRenderedMessage());
            }
            else {
            	String data [] = event.getRenderedMessage().split("#");
            	if (data != null && data[0]!=null) {
            		for (int i=0; i<data.length-1; i=i+2) 
            		{
	            		json.put(data[i], data[i+1]);	            			            		
            		}
            	}
            	else {
            		json.put("message",  event.getRenderedMessage());
            	}
            }
        }

        protected void writeThrowable(Map<String, Object> json, LoggingEvent event) {
            ThrowableInformation ti = event.getThrowableInformation();
            if (ti != null) {
                Throwable t = ti.getThrowable();
                json.put("className", t.getClass().getCanonicalName());
                json.put("stackTrace", getStackTrace(t));
            }
        }
        private void enchanceMapData(Map<String, Object> json, String complexStr) {
        	String result [] = complexStr.split("#");
            for (int i=0; i<result.length; ++i) {
        	                  if (i==0)
        	                  {
        	                       json.put("myTime", new Date().getTime());        	                       
        	                  }
        	                  else {
        	                       json.put("results"+i, result[i]);        	                       
        	                  }
        	             }
        }

        protected String getStackTrace(Throwable aThrowable) {
            final Writer result = new StringWriter();
            final PrintWriter printWriter = new PrintWriter(result);
            aThrowable.printStackTrace(printWriter);
            return result.toString();
        }

        /**
         * Method is called by ExecutorService and insert the document into
         * ElasticSearch
         *
         * @return
         * @throws Exception
         */        
        public LoggingEvent call() throws Exception {
            try {
                if (client != null) {
                    // Set up the es index response 
                    String uuid = UUID.randomUUID().toString();
                    Map<String, Object> data = new HashMap<String, Object>();

                    writeBasic(data, loggingEvent);
                    writeThrowable(data, loggingEvent);
                    // insert the document into elasticsearch
                    Index index = new Index.Builder(data).index(getIndex()).type(getType()).id(uuid).build();
                    client.execute(index);
                }
            } catch (Exception ex) {
            }
            return loggingEvent;
        }
    }
    
    public void setMessage(String test)
    {
        message = test;
    }
    public String getMessage() {
        return message;
    }

    public void setMyTime(long t)
    {
    	myTime = t;
    }
    public long getMyTime()
    {
    	return myTime;
    }
   
    public boolean getReels()
    {
         return reels;
    }
    public void setReels(boolean r)
    {
         reels = r;
    }

    public void setDt(String test)
    {
         Date current;
         try {
              current = new SimpleDateFormat().parse(test);
         }
         catch (Exception exc)
         {
              current = new Date();
         }
        dt = current;
    }
    public Date getDt() {
        return dt;
    }
}