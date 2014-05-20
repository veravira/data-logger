REGISTER '../../../lib/piggybank.jar';
        REGISTER '../../../lib/elasticsearch-hadoop-1.3.0.BUILD-SNAPSHOT.jar';
        define ESStorage org.elasticsearch.hadoop.pig.ESStorage('es.resource=vera/test, es.mapping.names=date:@timestamp');


        --a = LOAD 'outcomes.log' USING JsonLoader('reels:{(symbol:chararray,yoffset:int)}, creditsTotal:int, creditsWagered:int, creditsWon:int, denom:(value:double), currencyTotal:(value:double),           currencyWagered:(value:double), currencyWon:(value:double), restrictedCurrencyTotal:double, wagerOutcome:chararray, time:chararray');
a = LOAD 'out1.log' USING JsonLoader('reels:{(symbol:chararray,yoffset:int)}, creditsTotal:int, creditsWagered:int, creditsWon:int, denom:(value:double), currencyTotal:(value:double),                 currencyWagered:(value:double), currencyWon:(value:double), restrictedCurrencyTotal:double, wagerOutcome:chararray, time:chararray');

        DESCRIBE a;
        b = foreach a generate $0..$9, ToDate($10,  'MM/dd/yyyy HH:mm') as (dt:datetime);
        b1 = foreach b generate BagToTuple($0.$0) as r, $1 as ct, $2 as cw, $3 as cwon, $9 as status, $10 as dt;
        b2  = foreach b1 generate flatten($0), $1, $2, $3, $4, $5;
--      store b2 into 'b2';
        b3 = load 'b2' using PigStorage('\t') as (r1:chararray, r2:chararray, r3:chararray, ct: int,cw: int,cwon: int,status: chararray,dt: datetime);
        B = foreach b3 generate $0, $1, $2, $3, $4, $5, $6,$7;
        --dump B;


        b4 = group B by ($0, $1,$2);
        --dump b4;
        --b5 = foreach b4 generate group, COUNT($1), flatten($1.$3), flatten($1.$4), flatten($1.$5), flatten($1.$6);
        b5 = foreach b4 generate group, COUNT($1);
        c5 = foreach b4 generate group, $1.$4, $1.$5, $1.$7;

        ctDups = foreach b4 generate group, flatten($1.$3);
        ct = distinct ctDups;


        cwDups = foreach b4 generate group, flatten($1.$4);
        cw = distinct cwDups;


        cwonDups = foreach b4 generate group, flatten($1.$5);
        cwon = distinct cwonDups;
        time = foreach b4 generate group, flatten($1.$7);
        t = distinct time;



        ct_cw_dups = join ct by $0, cw by $0;
        ct_cw = distinct ct_cw_dups;
        --dump ct_cw;

        ct_cw_d = foreach ct_cw generate $0, $1, $3;
        --dump ct_cw_d;
/*
*
data1 = join ct_cw_d by $0, b5 by $0;
data2 = distinct data1;
data3 = foreach data2 generate $0, $1, $2, $4;
dump data3;
*
*/
        org_cw_dups = join b5 by $0, cw by $0;
        org_cw = distinct org_cw_dups;
        org_cw_d = foreach org_cw generate $0, $1, $3;
        pt1 = order org_cw_d by $1 desc;
        pt = foreach pt1 generate $0 as reels, (int)$1 as counter, $2 as creditsWon;
        dump pt;
        store pt into 'grps' using JsonStorage();
        describe pt;
        --b7 = order b5 by $1 desc;


        --dump b7;
        --b8 = distinct b7;
        --dump b8;

        store pt into 'vera/pt' USING ESStorage('es.http.timeout = 5m    es.index.auto.create = false');
        
