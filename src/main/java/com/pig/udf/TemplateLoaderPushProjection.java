package com.pig.udf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


/**
 * Template for creating a Loader that implements push projection
 */
public class TemplateLoaderPushProjection extends LoadFunc implements LoadPushDown {

    protected RecordReader reader = null;
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private boolean[] requiredFields = null;
    private boolean requiredFieldsInitialized = false;
    private String udfContextSignature = null;
    private static final String REQUIRED_FIELDS_SIGNATURE = "pig.templateloaderpushprojection.required_fields";

    private String inputFormatClassName = null;
    private String loadLocation;

    public TemplateLoaderPushProjection() {
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        loadLocation = location;
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        // if not manually set in options string
        if (inputFormatClassName == null) {
            if (loadLocation.endsWith(".bz2") || loadLocation.endsWith(".bz")) {
                inputFormatClassName = Bzip2TextInputFormat.class.getName();
            } else {
                inputFormatClassName = TextInputFormat.class.getName();
            }
        }
        try {
            return (FileInputFormat) PigContext.resolveClassName(inputFormatClassName).newInstance();
        } catch (InstantiationException e) {
            throw new IOException("Failed creating input format " + inputFormatClassName, e);
        } catch (IllegalAccessException e) {
            throw new IOException("Failed creating input format " + inputFormatClassName, e);
        }
    }

    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
        reader = recordReader;
        if (!requiredFieldsInitialized) {
            UDFContext udfc = UDFContext.getUDFContext();
            Properties p = udfc.getUDFProperties(this.getClass(), new String[] { udfContextSignature });
            requiredFields = (boolean[]) ObjectSerializer.deserialize(p.getProperty(REQUIRED_FIELDS_SIGNATURE));
            requiredFieldsInitialized = true;
        }
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            List values = new ArrayList();
            if (!reader.nextKeyValue()) {
                return null;
            }
            Text value = (Text) reader.getCurrentValue();

            //TODO: parse record into component fields, add to values in order
            //check that the appropriate index of requiredFields is true before adding

            return tupleFactory.newTuple(values);
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
        }
    }



    public List<OperatorSet> getFeatures() {
        return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
    }

    @Override
    public void setUDFContextSignature( String signature ) {
        udfContextSignature = signature;
    }

    public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList) throws FrontendException {
        if (requiredFieldList == null) {
            return null;
        }
        if (requiredFieldList.getFields() != null) {
            int lastColumn = -1;
            for (RequiredField rf : requiredFieldList.getFields()) {
                if (rf.getIndex() > lastColumn) {
                    lastColumn = rf.getIndex();
                }
            }
            requiredFields = new boolean[lastColumn + 1];
            for (RequiredField rf : requiredFieldList.getFields()) {
                if (rf.getIndex() != -1)
                    requiredFields[rf.getIndex()] = true;
            }
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
            try {
                p.setProperty(REQUIRED_FIELDS_SIGNATURE, ObjectSerializer.serialize(requiredFields));
            } catch (Exception e) {
                throw new RuntimeException("Cannot serialize mRequiredColumns");
            }

        }

        return new RequiredFieldResponse(true);
    }
}
