/**
* this script gets a file in json format that has only reels win/lose information (clean json preprocessed data here)
* format sample below
* {"reels":[{"symbol":"Single bar","yoffset":-2200},{"symbol":"Triple bars","yoffset":-1800},{"symbol":"Blank","yoffset":-100}],"creditsTotal":999,"creditsWagered":1,"creditsWon":0,"denom":{"value":1.00},"currencyTotal":{"value":999.00},"currencyWagered":{"value":1.00},"currencyWon":{"value":0.00},"restrictedCurrencyTotal":null,"wagerOutcome":"LOSE","time":"12/16/2013 12:20"}}
* {"reels":[{"symbol":"Single bar","yoffset":-2200},{"symbol":"Jackpot","yoffset":-1000},{"symbol":"Double bars","yoffset":-200}],"creditsTotal":1003,"creditsWagered":1,"creditsWon":5,"denom":{"value":1.00},"currencyTotal":{"value":1003.00},"currencyWagered":{"value":1.00},"currencyWon":{"value":5.00},"restrictedCurrencyTotal":null,"wagerOutcome":"WIN","time":"12/16/2013 12:20"}}
* {"reels":[{"symbol":"Seven","yoffset":-1600},{"symbol":"Cherry","yoffset":-1200},{"symbol":"Blank","yoffset":-1900}],"creditsTotal":1003,"creditsWagered":1,"creditsWon":1,"denom":{"value":1.00},"currencyTotal":{"value":1003.00},"currencyWagered":{"value":1.00},"currencyWon":{"value":1.00},"restrictedCurrencyTotal":null,"wagerOutcome":"WIN","time":"12/16/2013 12:20"}}
* created by vera
*
* loads the data and ships it to the ES via ESStorage call
**/

	REGISTER '../../../lib/elasticsearch-hadoop-1.3.0.BUILD-SNAPSHOT.jar';
	define ESStorage org.elasticsearch.hadoop.pig.ESStorage('es.resource=prototype/test, es.mapping.names=date:@timestamp');
	-- ES index = prototype and type =test
	a = LOAD 'test.log' USING JsonLoader('reels:{(symbol:chararray,yoffset:int)}, creditsTotal:int, creditsWagered:int, creditsWon:int, denom:(value:double), currencyTotal:(value:double), 		currencyWagered:(value:double), currencyWon:(value:double), restrictedCurrencyTotal:(value:double), wagerOutcome:chararray, time:chararray');
	DESCRIBE a;
	--b = foreach a generate $0..$9, ToDate($10,  'MM/dd/yyyy HH:mm:ss.SSS') as (dt:datetime);
	b = foreach a generate $0..$9, ToDate($10,  'MM/dd/yyyy HH:mm') as (dt:datetime);
	--dump b;
	b1 = foreach b generate BagToTuple($0.$0) as r, $1 as ct, $2 as cw, $3 as cwon, $9 as status, $10 as dt;
	b2  = foreach b1 generate flatten($0), $1, $2, $3, $4, $5;
	--store b2 into 'bb2';
	b3 = load 'bb2' using PigStorage('\t') as (reel1:chararray, reel2:chararray, reel3:chararray, currencytotal: int, creditsWagered: int, creditswon: int,status: chararray,dt: datetime);
	B = foreach b3 generate $0, $1, $2, $3, $4, $5, $6,$7;
	--dump B;
	BB = distinct B;
/**
* the results are in the format example below
*(Cherry,Cherry,Cherry,1093,1,5,WIN,2013-12-12T15:34:00.000-08:00)
**/
store BB into 'prototype/test' USING ESStorage('es.http.timeout = 5m 	 es.index.auto.create = false');

	--b4 = group B by ($0, $1,$2);
	--dump b4;
	--b5 = foreach b4 generate group, COUNT($1), flatten($1.$3), flatten($1.$4), flatten($1.$5), flatten($1.$6);
	
	