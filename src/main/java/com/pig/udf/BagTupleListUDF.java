package com.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.base.Splitter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Example UDF that takes two integers and returns a bag with two tuples: one containing the
 * product and one the quotient.  Written to show how to use bags and tuples, not because
 * it's useful in any way.
 */
public class BagTupleListUDF extends EvalFunc<DataBag> {

    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();


    @Override
    public DataBag exec(Tuple tuple) throws IOException {
        // expect one string
        if (tuple == null) {
            throw new IllegalArgumentException("BagTupleExampleUDF: requires two input parameters.");
        }
        try {
            String data = DataType.toString(tuple.get(0));
            System.out.println("***** = " + data);
            Iterable<String> dd = Splitter.on(',')
         	       .trimResults()
        	       .omitEmptyStrings()
        	       .split(data);
            //            String second  = DataType.toString(tuple.get(1));

            DataBag output = mBagFactory.newDefaultBag();
            for(String cur:dd) {
            	System.out.println("***** = " + cur);
            	output.add(mTupleFactory.newTuple(Collections.singletonList(cur)));
            }
//            output.add(mTupleFactory.newTuple(Collections.singletonList(second)));
            return output;
        }
        catch (Exception e) {
            throw new IOException("BagTupleExampleUDF: caught exception processing input.", e);
        }
    }

    public Schema outputSchema(Schema input) {
        // Function returns a bag with this schema: { (Double), (Double) }
        // Thus the outputSchema type should be a Bag containing a Double
        try{
            Schema bagSchema = new Schema();
            bagSchema.add(new Schema.FieldSchema("my_tuples", DataType.CHARARRAY));

            String schemaName =  getSchemaName("seq", input);
            return new Schema(new Schema.FieldSchema(schemaName, bagSchema, DataType.BAG));
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}
