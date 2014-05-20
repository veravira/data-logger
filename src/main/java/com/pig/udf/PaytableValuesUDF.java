package com.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;


public class PaytableValuesUDF extends EvalFunc<Long> {
	
	private double multipyBy(int factor){
		double value = 1;
		if (factor<0) {
			value = (-1d)/factor;
		}  
		else 
			value = factor;
		return value;
	}

    @Override
    public Long exec(Tuple input) throws IOException {
    	Long result= new Long(1);
    	System.out.println("Tuple ... = " + input);
        // expect one string
        if (input == null) {
            System.out.println("PaytableUDF: requires one input parameter.");
        }
        try {
        	int paytableNumber= 0;
        	paytableNumber = (Integer)input.get(1);
            String symbols = (String) input.get(0);
            System.out.println("********************Tuple ... = " + symbols);
            System.out.println("********************Tuple ... = " + paytableNumber);
            double factor = multipyBy(paytableNumber);
            switch(symbols){
            	case "jjj":
            		result = (long)(1500 * factor);
            	break;
            	case "jj-":
            		result = (long)(50 * factor);
            	break;
            	case "j--":
            		result = (long)(5 * factor);
            	break;
            	case "ccc":
            		result = (long)(25 * factor);
            	break;
            	case "cc-":
            		result = (long)(5* factor);
            	break;
            	case "c--":
            		result = (long)(1* factor);
            	break;
            	case "777":
            		result = (long)(500* factor);
            	break;
            	case "b3b3b3":
            		result = (long)(90* factor);
            	break;
            	case "b2b2b2":
            		result = (long)(50* factor);
            	break;
            	case "b1b1b1":
            		result = (long)(30* factor);
            	break;
            	case "ababab":
            		result = (long)(10* factor);
            	break;
            }
            System.out.println("********************"+ result);
            return result;
        }
        catch (ExecException e) {
            throw new IOException("Paytable: caught exception processing input.", e);
        }

    }
}
