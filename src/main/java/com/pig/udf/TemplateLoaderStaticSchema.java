package com.pig.udf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.pig.ResourceSchema.ResourceFieldSchema;
import static org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Template for Loader that implements push projection and assumes a static schema definition
 */
public class TemplateLoaderStaticSchema extends LoadFunc implements LoadMetadata, LoadPushDown {

    protected RecordReader reader = null;
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    protected ResourceFieldSchema[] fields = null;
    private boolean[] requiredFields = null;
    private boolean requiredFieldsInitialized = false;
    private String udfContextSignature = null;
    private static final String REQUIRED_FIELDS_SIGNATURE = "pig.templateloaderstaticschema.required_fields";

    private String inputFormatClassName = null;
    private String loadLocation;

    public TemplateLoaderStaticSchema() {
        fields = getSchema().getFields();
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
            if (value != null) {

                //TODO: split input into component fields
                //For example:
//                String[] parts = fieldDelimiter.split(value.toString());
//                if (requiredFields != null) {
//                    values = getValues(parts, requiredFields);
//                } else {
//                    values = getValues(parts);
//                }
            }
            return tupleFactory.newTuple(values);
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
        }
    }



    public ResourceSchema getSchema(String s, Job job) throws IOException {
        return getSchema();
    }


    private ResourceSchema getSchema() {
        List<FieldSchema> fieldSchemaList = new ArrayList<FieldSchema>();

        fieldSchemaList.add(new FieldSchema("id", DataType.LONG));
        //TODO: add fields here

        return new ResourceSchema(new Schema(fieldSchemaList));
    }

    public ResourceStatistics getStatistics(String s, Job job) throws IOException {
        return null;
    }

    public String[] getPartitionKeys(String s, Job job) throws IOException {
        return null;
    }

    public void setPartitionFilter(Expression expression) throws IOException {
        // intentionally not implemented
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
        if (requiredFieldList.getFields() != null)
        {
            requiredFields = new boolean[fields.length];

            for (RequiredField f : requiredFieldList.getFields()) {
                requiredFields[f.getIndex()] = true;
            }

            UDFContext udfc = UDFContext.getUDFContext();
            Properties p = udfc.getUDFProperties(this.getClass(), new String[]{ udfContextSignature });
            try {
                p.setProperty(REQUIRED_FIELDS_SIGNATURE, ObjectSerializer.serialize(requiredFields));
            } catch (Exception e) {
                throw new RuntimeException("Cannot serialize requiredFields for pushProjection");
            }
        }

        return new RequiredFieldResponse(true);
    }
}