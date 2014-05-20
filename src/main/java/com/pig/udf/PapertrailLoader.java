package com.pig.udf;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
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
import java.util.regex.Pattern;

import static org.apache.pig.ResourceSchema.ResourceFieldSchema;
import static org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * A loader for Papertrail Logs that assumes the default tab-separated schema of:
 *  id
 *  generated_at
 *  source_id
 *  source_name
 *  source_ip
 *  facility_name
 *  severity_name
 *  message
 * where anything that occurs after severity_name is considered to be part of message,
 * and tabs are preserved within the message.   Takes no schema because schema is assumed.
 */
public class PapertrailLoader extends LoadFunc implements LoadMetadata, LoadPushDown {

    protected RecordReader reader = null;
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private static final Pattern fieldDelimiter = Pattern.compile("\t");
    protected ResourceFieldSchema[] fields = null;
    private boolean[] requiredFields = null;
    private boolean requiredFieldsInitialized = false;
    private String udfContextSignature = null;
    private static final String REQUIRED_FIELDS_SIGNATURE = "pig.papertrailloader.required_fields";

    private final static Options validOptions_ = new Options();
    private final static CommandLineParser parser_ = new GnuParser();
    private String inputFormatClassName = null;
    private String loadLocation;

    public PapertrailLoader() {
        this("");
    }

    public PapertrailLoader(String optStr) {
        populateValidOptions();
        String[] optsArr = optStr.split(" ");
        try {
            CommandLine configuredOptions_ = parser_.parse(validOptions_, optsArr);
            if (configuredOptions_.getOptionValue("inputFormat") != null) {
                this.inputFormatClassName = configuredOptions_.getOptionValue("inputFormat");
            }
        } catch (org.apache.commons.cli.ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "[-inputFormat]", validOptions_ );
            throw new RuntimeException(e);
        }
        fields = getSchema().getFields();
    }


    private static void populateValidOptions() {
        validOptions_.addOption("inputFormat", true, "The input format class name" +
                " used by this loader instance");
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
            if(loadLocation.endsWith(".bz2") || loadLocation.endsWith(".bz")) {
                return new Bzip2TextInputFormat();
            } else {
                return new PigTextInputFormat();
            }
        }
        try {
            return (FileInputFormat) PigContext.resolveClassName(inputFormatClassName).newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed creating input format " + inputFormatClassName, e);
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
                String[] parts = fieldDelimiter.split(value.toString(), 9);
                if (requiredFields != null) {
                    values = getValues(parts, requiredFields);
                } else {
                    values = getValues(parts);
                }
            }
            return tupleFactory.newTuple(values);
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
        }
    }


    private List getValues(String[] parts) {
        List values = new ArrayList();
        for (int i = 0; i < parts.length; i++) {
            values.add(parts[i]);
        }
        return values;
    }

    private List getValues(String[] parts, boolean[] requiredFields) {
        List values = new ArrayList();
        for (int i = 0; i < parts.length; i++) {
            if (requiredFields[i]) {
                values.add(parts[i]);
            }
        }
        return values;
    }


    public ResourceSchema getSchema(String s, Job job) throws IOException {
        return getSchema();
    }


    private ResourceSchema getSchema() {
        List<FieldSchema> fieldSchemaList = new ArrayList<FieldSchema>();

        fieldSchemaList.add(new FieldSchema("id", DataType.LONG));
        fieldSchemaList.add(new FieldSchema("generated_at", DataType.CHARARRAY));
        fieldSchemaList.add(new FieldSchema("received_at", DataType.CHARARRAY));
        fieldSchemaList.add(new FieldSchema("source_id", DataType.CHARARRAY));
        fieldSchemaList.add(new FieldSchema("source_name", DataType.CHARARRAY));
        fieldSchemaList.add(new FieldSchema("source_ip", DataType.CHARARRAY));
        fieldSchemaList.add(new FieldSchema("facility_name", DataType.CHARARRAY));
        fieldSchemaList.add(new FieldSchema("severity_name", DataType.CHARARRAY));
        fieldSchemaList.add(new FieldSchema("message", DataType.CHARARRAY));

        return new ResourceSchema(new Schema(fieldSchemaList));
    }

    public ResourceStatistics getStatistics(String s, Job job) throws IOException {
        return null;
    }

    public String[] getPartitionKeys(String s, Job job) throws IOException {
        return null;
    }

    public void setPartitionFilter(Expression expression) throws IOException {
        // empty
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