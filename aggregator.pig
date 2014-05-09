-- this script is an exilirily script to compibe the data results from (pricePredictor.pig) and join it to the initail set
-- To Run the script
-- pig -x local -f aggregator.pig -param filename=small1.csv

REGISTER '../../../lib/elasticsearch-hadoop-pig.jar';
define ESStorage org.elasticsearch.hadoop.pig.EsStorage('es.resource=test/likes');
 
data = load '$filename' using PigStorage(',');
split data into cols2 if ($2 == NULL OR SIZE($2)<1), moreCols if (SIZE($2)>1);
cols2 = foreach data generate (chararray) $0, (chararray)$1;

mc = foreach moreCols generate $0, {$1..};
mc2 = foreach mc generate $0, $1, COUNT($1) as col_size;
-- mc2 stores clusters of likes per UID

mm = FOREACH mc2 {
    uniq = DISTINCT $1;
        GENERATE $0, FLATTEN(uniq) as word;
};
--store mm into 'mm';
mm2 = load 'mm' using PigStorage('\t') as (uid: chararray, like: chararray);

data1 = union cols2, mm2; 
-- ship data to elasticsearch (had to be install already)
store data1 into 'test/scopely-all' USING ESStorage('es.http.timeout = 5m      es.index.auto.create = false');

--store data1 into 'data1';  
freq1 = group data1 by $1;
fr1 = foreach freq1 generate group, COUNT($1.$0);
fr2 = order fr1 by $1 desc;

-- store to local disk
--store fr2 into 'fr2';
fr50 = filter fr2 by $1>=50;

--store fr50 into 'fr50';  
--store fr50 into 'test/scopely' USING ESStorage('es.http.timeout = 5m      es.index.auto.create = false');
