-- pig -x local -f shipResultsToEs.pig -param filename=file.csv
--REGISTER '/Users/vera.kalinichenko/soft/elasticsearch-hadoop-1.3.0.M3.jar';
REGISTER '/Users/vera.kalinichenko/soft/elasticsearch-hadoop-2.0.0.RC1/dist/elasticsearch-hadoop-2.0.0.RC1.jar';
REGISTER '/Users/vera.kalinichenko/soft/elasticsearch-hadoop-2.0.0.RC1/dist/elasticsearch-hadoop-pig-2.0.0.RC1.jar';
REGISTER '../../../target/pig-template-1.0-SNAPSHOT.jar';

define EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.resource=test/seq');
define myBag com.gamblitgaming.pig.BagTupleListUDF;

data = load '/Users/vera.kalinichenko/data/datascience/rc.os/logs/outcomes.log' using PigStorage('\n') as ( line: chararray);

words = foreach data generate flatten($0);
dump words;
describe words;



w = foreach words generate myBag(line);
describe w;
w1 = foreach w generate $0;
dump w1;
store w1 into 'test/seq' USING EsStorage('es.http.timeout = 5m      es.index.auto.create = false');