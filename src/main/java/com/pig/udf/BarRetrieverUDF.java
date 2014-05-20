package com.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.BinSedesTuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.base.Splitter;

import java.io.IOException;
import java.util.UUID;

/**
 * Example UDF that takes two integers and returns a bag with two tuples: one containing the
 * product and one the quotient.  Written to show how to use bags and tuples, not because
 * it's useful in any way.
 */
public class BarRetrieverUDF extends EvalFunc<Tuple> {

    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();


    @Override
    public Tuple exec(Tuple tuple) throws IOException {
        // expect one string
        if (tuple == null || tuple.size()<2) {
        	if (tuple!=null) { 
        		System.out.println("**** " + tuple.size());
        		System.out.println("**** " + tuple.get(0));
        	}
        		
            throw new IllegalArgumentException("BarRetrieverUDF: requires two input parameters.");
        }
        try {
//        	DataBag bag = (DataBag)tuple.get(0);
//        	while (bag.iterator().hasNext())
//        	{
//        		String first = (String) bag.iterator().next().get(0);
//        	}
            String first = (String)tuple.get(0);
            long id = (Long)tuple.get(1);
            String  temp = null;
//            for (int i = 0; i< first.size(); ++i) {
//            	temp = (String)first.get(i);
//            	System.out.print("Splitter = " + temp + " ");
//            }
            System.out.println("**** " + first);
            int index = first.indexOf("<");
            int end = first.indexOf(">>");
            String modified; 
            if (index>=0 && end>=0) {
            	String value = first.substring(index+2, end);
//            	modified = shipCurlToES(value, id);
            	modified = shipCurlToPython(value, id);
            }
            else {
            	modified = first;
            }
            Tuple t = mTupleFactory.newTuple(modified);
            return t;
        }
        catch (Exception e) {
            throw new IOException("BarRetrieverUDF: caught exception processing input.", e);
        }
    }
    
    private String shipCurlToES(String actual, long id) {
    	String constructed = "";
    	StringBuffer sb = new StringBuffer(); 
    	UUID someId = new UUID(16, 4);
    	sb.append("curl -XPUT \'http://localhost:9200/logs\'");
//    	sb.append(id).append("\'").append(" -d ' \n");
    	sb.append(" -d ' ");
    	sb.append("{");
    	sb.append("\"McpSessionClientID\": \"").append(UUID.randomUUID().toString() + "\""+ ",");
    	sb.append("\"gamebar\": ").append(actual).append("\""); 
    	sb.append("}'");
    	constructed = sb.toString();
    	System.out.println(sb.toString());
    	return constructed;
    }
    private String shipCurlToPython(String actual, long id) {
    	String constructed = "";
    	StringBuffer sb = new StringBuffer(); 
    	UUID someId = new UUID(16, 4);
    	sb.append("\'http://localhost:9200/logs\'");
//    	sb.append(id).append("\'").append(" -d ' \n");
    	sb.append(" -d ' ");
    	sb.append("{");
    	sb.append("\"McpSessionClientID\": \"").append(UUID.randomUUID().toString() + "\""+ ",");
//    	sb.append("\"gamebar\": ").append(actual).append("\"");
    	sb.append("\"creditsTotal\"=99");
    	sb.append("}'");
    	constructed = sb.toString();
    	System.out.println(sb.toString());
    	return constructed;
    }

    
    public Schema outputSchema(Schema input) {
        // Function returns a bag with this schema: { (Double), (Double) }
        // Thus the outputSchema type should be a Bag containing a Double
        try{
            Schema bagSchema = new Schema();
            bagSchema.add(new Schema.FieldSchema("result_field", DataType.DOUBLE));

            String schemaName =  getSchemaName(this.getClass().getName().toLowerCase(), input);
            return new Schema(new Schema.FieldSchema(schemaName, bagSchema, DataType.BAG));
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}
