-- pig -x local -f shipResultsToEs.pig -param filename=file.csv
--REGISTER '/Users/vera.kalinichenko/soft/elasticsearch-hadoop-1.3.0.M3.jar';
REGISTER '/Users/vera.kalinichenko/soft/elasticsearch-hadoop-2.0.0.RC1/dist/elasticsearch-hadoop-2.0.0.RC1.jar';
REGISTER '/Users/vera.kalinichenko/soft/elasticsearch-hadoop-2.0.0.RC1/dist/elasticsearch-hadoop-pig-2.0.0.RC1.jar';

define EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.resource=test/loss');


data = load '/Users/vera.kalinichenko/data/datascience/rc.os/logs/losses.log' using PigStorage(';') as ( line: chararray, losses:chararray);

store data into 'test/loss' USING EsStorage('es.http.timeout = 5m      es.index.auto.create = false');