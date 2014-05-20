
	REGISTER '../../../lib/elasticsearch-hadoop-1.3.0.BUILD-SNAPSHOT.jar';
	REGISTER '../../../target/pig-template-1.0-SNAPSHOT.jar';

	DEFINE mySplitter com.gamblitgaming.pig.SplitterUDF;

	define ESStorage org.elasticsearch.hadoop.pig.ESStorage('es.resource=sample/bars1');
	
	
	data = load 'data/gw.os.log1' using PigStorage('\t'); 

 	gameObjsData = filter data by ($2 MATCHES '.*reels.*'); 	

 	reels = foreach gameObjsData generate (chararray)$2;
 	test = foreach reels generate $0, SIZE($0);
 	DUMP test;

 	split reels into data1 if SIZE($0)<53, data2 if (SIZE($0)>53); 

 	cl_rls = foreach data2 generate mySplitter($0);
 	d = foreach cl_rls generate flatten($0);
 	
store d into 'd1';

a = LOAD 'd1' USING JsonLoader('reels:{(symbol:chararray,yoffset:int)}');
DESCRIBE a;
DUMP a;

	
	--store a into 'sample/bars1' USING ESStorage('es.http.timeout = 5m 	 es.index.auto.create = false');
	

	--STORE c INTO 'bar.json' USING JsonStorage();
	--j = load 'bar.json' using JsonLoader('dt: chararray, credit: chararray, bars: chararray');
	
	--STORE j INTO 'sample/bars' USING ESStorage('es.http.timeout = 5m 	 es.index.auto.create = false');
