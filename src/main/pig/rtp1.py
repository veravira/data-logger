#!/usr/bin/python
from org.apache.pig.scripting import *
from subprocess import call
def main():
    
    output = "output/data_0"

Prep = Pig.compile("""
    REGISTER '/Users/vera.kalinichenko/soft/piggybank.jar';
    REGISTER '../../../lib/elasticsearch-hadoop-1.3.0.BUILD-SNAPSHOT.jar';
    define ESStorage org.elasticsearch.hadoop.pig.ESStorage('es.resource=gwos/bars');
    
    data = load '$file' using PigStorage('\t'); 
    a = LOAD 'test.log' USING JsonLoader('reels:{(symbol:chararray,yoffset:int)}, creditsTotal:int, creditsWagered:int, creditsWon:int, denom:(value:double), currencyTotal:(value:double),         currencyWagered:(value:double), currencyWon:(value:double), restrictedCurrencyTotal:(value:double), wagerOutcome:chararray, time:chararray');
    
    --b = foreach a generate $0..$9, ToDate($10,  'MM/dd/yyyy HH:mm') as (dt:datetime);
    b = foreach a generate $0..$9, ToDate($10,  'MM/dd/yyyy HH:mm:ss.SSS') as (dt:datetime);
    
    b1 = foreach b generate BagToTuple($0.$0) as r, $1 as ct, $2 as cw, $3 as cwon, $9 as status, $10 as dt;
    b2  = foreach b1 generate flatten($0), $1, $2, $3, $4, $5;
    store b2 into 'bb2/$path';
    b3 = load 'bb2/$path' using PigStorage('\t') as (reel1:chararray, reel2:chararray, reel3:chararray, currencytotal: int, creditsWagered: int, creditswon: int,status: chararray,dt: datetime);
    B = foreach b3 generate $0, $1, $2, $3, $4, $5, $6,$7;
    --dump B;
    BB = distinct B;
    /**
    * the results are in the format example below
    *(Cherry,Cherry,Cherry,1093,1,5,WIN,2013-12-12T15:34:00.000-08:00)
    **/
    store BB into 'rtp-test/$path' USING ESStorage('es.http.timeout = 5m      es.index.auto.create = false'); 
    dump BB;

"""
)

Sorted = Pig.compile("""
    data = load '$freq' using PigStorage('\t') as (word:chararray, freq:int);
    sorted = order data by $1 desc;
    some = limit sorted 100;
    results = foreach some generate $0;
    store results into 'r';
    data1 = load '$fileName' using PigStorage('\t') as (oid_:long, tit:chararray, zpid:long);
    data = foreach data1 generate $0 as oid:long, LOWER($1) as title:chararray;
    a = foreach data generate $0 as id, $1 as gg;

    app2 = foreach a generate id, gram(2,gg);
    app3 = foreach app2 generate $0, flatten($1) as gr2;
    aaH = filter app3 by $1 matches '$w .*';

    BH = foreach (group aaH all) generate COUNT(aaH) as total;
    C1H = foreach (group aaH by $1) generate group as x, 100* (double)COUNT(aaH) / (double) BH.total as perc, flatten($1.$0) as id;
    C2H = group C1H by $0;
    C3H = foreach C2H generate '$w',$0, MIN($1.$1), SIZE($1.$2);
    C4H = order C3H by $2 desc;

    store C4H into 'out/$w';


"""
)
# .bind().runSingle()

params = {'file':'data', 'path':'path'}
file = open('data/fileList.txt', 'rU')
r = file.readline()
i = 0
# boundF = Prep.bind();
#     job2 = boundF.runSingle();
#     if not job2.isSuccessful():
#         raise 'failed'
        
while (r is not None):
    w = r.split('\t')[1]
    name = r.split('\t')[0]    
    print "*****************************" + w;
    print "file = " + name;        
    params["path"]=w
    params["file"]=name
    r = file.readline()
    bound = Prep.bind(params)
    stats = bound.runSingle()

file.close

if __name__ == '__main__':
    main()