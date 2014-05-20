package com.pig.udf;

import java.io.IOException;
import java.util.Properties;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;

public class SplitterUDF extends EvalFunc<Tuple>{
	TupleFactory mTupleFactory = TupleFactory.getInstance();
	public SplitterUDF() {	
	}
	
	public Tuple exec(Tuple input) throws IOException {
		String output = null;
		Tuple newTuple = mTupleFactory.getInstance().newTuple();
		String temp = (String) input.get(0);
//		System.out.println(temp + " Tuple value ..");
		int index = temp.indexOf(": ");
		output = temp.substring(index + 3);
//		System.out.println("**********====>" + output);
		int index2 = output.indexOf("'");	
//		System.out.println("**********====>" + index2);
		output = output.substring(0, index2);
//		System.out.println("**********====>" + output);
		newTuple.append(output);
		System.out.println("*********" + output);
		return newTuple;
	}
	public Schema outputSchema(Schema input) {       
        try{
            Schema bagSchema = new Schema();
            FieldSchema symbol1 = new Schema.FieldSchema("symbol1", DataType.CHARARRAY);
            FieldSchema symbolOffset1 = new Schema.FieldSchema("offset1", DataType.INTEGER);
            bagSchema.add(new Schema.FieldSchema("reels_field", DataType.TUPLE));
            bagSchema.add(new Schema.FieldSchema("symbol", DataType.CHARARRAY));
            bagSchema.add(new Schema.FieldSchema("offset", DataType.INTEGER));
            
            String schemaName =  getSchemaName(this.getClass().getName().toLowerCase(), input);
            
         // We must strip the outer {} or this schema won't parse on the back-end
            String schemaString = input.toString().substring(1, input.toString().length() - 1);

            // Set the input schema for processing
            UDFContext context = UDFContext.getUDFContext();
            Properties udfProp = context.getUDFProperties(this.getClass());

            udfProp.setProperty("com.gamblitgaming.json.udf.schema", schemaString);
            
            return new Schema(new Schema.FieldSchema(schemaName, bagSchema, DataType.BAG));
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

}
