#!/usr/bin/python
from org.apache.pig.scripting import *
from subprocess import call
def main():
    
    output = "output/data_0"

Prep = Pig.compile("""
    a = LOAD '$file' USING JsonLoader('reels:{(symbol:chararray,yoffset:int)}, creditsTotal:int, creditsWagered:int, creditsWon:int, denom:(value:double), currencyTotal:(value:double),         currencyWagered:(value:double), currencyWon:(value:double), restrictedCurrencyTotal:(value:double), wagerOutcome:chararray, time:chararray');
    
    b = foreach a generate $0..$9, ToDate($10,  '$format') as (dt:datetime);
    
    b1 = foreach b generate BagToTuple($0.$0) as r, $1 as ct, $2 as cw, $3 as cwon, $9 as status, $10 as dt;
    b2  = foreach b1 generate flatten($0), $1, $2, $3, $4, $5;
    store b2 into 'bb2/$path';
    b3 = load 'bb2/$path' using PigStorage('\t') as (reel1:chararray, reel2:chararray, reel3:chararray, currencytotal: int, creditsWagered: int, creditswon: int,status: chararray,dt: datetime);
    B = foreach b3 generate $0, $1, $2, $3, $4, $5, $6,$7;
    dump B;
    store B into 'data/tmp/$path';
"""
)
    
step1= Pig.compile("""
    b3 = load 'data/tmp/$path' using PigStorage('\t') as (reel1:chararray, reel2:chararray, reel3:chararray, currencytotal: int, creditsWagered: int, creditswon: int,status: chararray,dt: datetime);
    B = foreach b3 generate $0, $1, $2, $3, $4, $5, $6,$7;
    
    /**
    * the results are in the format example below
    *(Cherry,Cherry,Cherry,1093,1,5,WIN,2013-12-12T15:34:00.000-08:00)
    **/
    --store B into 'rtp-test-all-$path/$path' USING ESStorage('es.http.timeout = 5m      es.index.auto.create = false'); 
    
    /**
    * the results are in the format example below
    *(Cherry,Cherry,Cherry,1093,1,5,WIN,2013-12-12T15:34:00.000-08:00)
    **/
    BB = distinct B;
    cherry1 =  filter BB by $0=='Cherry';
    dataCh1 = foreach (group cherry1 all) generate 'Cherry', COUNT(cherry1) as ch1;
    
    sev1 =  filter BB by $0=='Seven';        
    data71 = foreach (group sev1 all) generate 'Seven', COUNT(sev1) as sev1;
    
    bar11 =  filter BB by $0=='Single bar';    
    dataBar11 = foreach (group bar11 all) generate 'Single bar', COUNT(bar11) as bar11;
    
    bar21 =  filter BB by $0=='Double bars';    
    dataBar21 = foreach (group bar21 all) generate 'Double bars', COUNT(bar21) as bar21;
    
    bar31 =  filter BB by $0=='Triple bars';    
    dataBar31 = foreach (group bar31 all) generate 'Triple bars', COUNT(bar31) as bar31;
    
    any1 =  filter BB by (($0 == 'Triple bars') OR ($0 == 'Single bar') OR ($0 == 'Double bars'));    
    dataAny1 = foreach (group any1 all) generate 'Any bars', COUNT(any1) as any1;
    
    j1 =  filter BB by $0=='Jackpot';    
    dataJ1 = foreach (group j1 all) generate 'Jackpot', COUNT(j1) as j1;
    
    b1 =  filter BB by $0=='Blank';    
    dataB1 = foreach (group b1 all) generate 'Blank', COUNT(b1) as b1;
    
    
    unionReel1 = union dataCh1, data71, dataB1, dataJ1, dataAny1,dataBar31, dataBar21, dataBar11;
    unionReel1_ = foreach unionReel1 generate $0, 1, $1; 
    describe unionReel1;
     
    --moreData1 =  join B by $0, unionReel1_ by $0; 
    
    cherry2 =  filter BB by $1=='Cherry';    
    dataCh2 = foreach (group cherry2 all) generate 'Cherry', COUNT(cherry2) as ch2;
    
    sev2 =  filter BB by $1=='Seven';        
    data72 = foreach (group sev2 all) generate 'Seven', COUNT(sev2) as sev2;
    
    bar12 =  filter BB by $1=='Single bar';    
    dataBar12 = foreach (group bar12 all) generate 'Single bar', COUNT(bar12) as bar12;
    
    bar22 =  filter BB by $1=='Double bars';    
    dataBar22 = foreach (group bar22 all) generate 'Double bars', COUNT(bar22) as bar22;
    
    bar32 =  filter BB by $1=='Triple bars';    
    dataBar32 = foreach (group bar32 all) generate 'Triple bars', COUNT(bar32) as bar32;
    
    any2 =  filter BB by (($1 == 'Triple bars') OR ($1 == 'Single bar') OR ($1 == 'Double bars'));    
    dataAny2 = foreach (group any2 all) generate 'Any bars', COUNT(any2) as any2;
    
    j2 =  filter BB by $1=='Jackpot';    
    dataJ2 = foreach (group j2 all) generate 'Jackpot', COUNT(j2) as j2;
    
    b2 =  filter BB by $1=='Blank';    
    dataB2 = foreach (group b2 all) generate 'Blank', COUNT(b2) as b2;
    
    
    unionReel2 = union dataCh2, data72, dataB2, dataJ2, dataAny2,dataBar32, dataBar22, dataBar12;
    unionReel2_ = foreach unionReel2 generate $0, 2, $1; 
    --moreData2 =  join B by $1, unionReel2_ by $0; 
    --dump moreData2;
    cherry3 =  filter BB by $2=='Cherry';    
    dataCh3 = foreach (group cherry3 all) generate 'Cherry', COUNT(cherry3) as ch3;
    
    sev3 =  filter BB by $2=='Seven';        
    data73 = foreach (group sev3 all) generate 'Seven', COUNT(sev3) as sev3;
    
    bar13 =  filter BB by $2=='Single bar';    
    dataBar13 = foreach (group bar13 all) generate 'Single bar', COUNT(bar13) as bar13;
    
    bar23 =  filter BB by $2=='Double bars';    
    dataBar23 = foreach (group bar23 all) generate 'Double bars', COUNT(bar23) as bar23;
    
    bar33 =  filter BB by $2=='Triple bars';    
    dataBar33 = foreach (group bar33 all) generate 'Triple bars', COUNT(bar33) as bar33;
    
    any3 =  filter BB by (($2 == 'Triple bars') OR ($2 == 'Single bar') OR ($2 == 'Double bars'));    
    dataAny3 = foreach (group any3 all) generate 'Any bars', COUNT(any3) as any3;
    
    
    
    j3 =  filter BB by $2=='Jackpot';    
    dataJ3 = foreach (group j3 all) generate 'Jackpot', COUNT(j3) as j3;
    
    b3 =  filter BB by $2=='Blank';    
    dataB3 = foreach (group b3 all) generate 'Blank', COUNT(b3) as b3;
    
    
    unionReel3 = union dataCh3, data73, dataB3, dataJ3, dataAny3,dataBar33, dataBar23, dataBar13;
    unionReel3_ = foreach unionReel3 generate $0, 3, $1; 
    un = union unionReel1_, unionReel2_, unionReel3_;

    store un into 'data/$path';

"""
)    

    ### this script uses raw data already pre parsed to do just the analytics and save the final results to ES (the actual RTP)###
    ### this is an utility to transfer resulst to ES after script test2.pig has been ran and raw data is saved in ES already###
###  (Seven,1,10,Seven,2,2,Seven,3,22,1,223,2,237,3,233)  ###
step2 = Pig.compile("""
    c = load 'data/$path' using PigStorage() as (symbol: chararray, reelNumber:int, total:long);
   dupC = foreach c generate *;
 
    cc1 = join c by $0, dupC by $0;
    noSelf = filter cc1 by ($1!=$4);
    ord1 = filter noSelf by ($1<$4); 
    noSelf1 = join ord1 by $0, c by $0;
    noSelf2 = filter noSelf1 by ($4 != $7 AND $4!=$1 AND $7!=$1);

    ord2 = filter noSelf2 by ($4<$7);
    t1 = group ord2 by $1;
    tt1 = foreach t1 generate group, SUM($1.$2);    
    t2 = group ord2 by $4;
    tt2 = foreach t2 generate group, SUM($1.$5);
    t3 = group ord2 by $7;
    tt3 = foreach t3 generate group, SUM($1.$8);
    
    T = cross tt1, tt2, tt3; 
    ord3 = join ord2 by $1, T by $0; 
-- INSERT THE CUT HERE-- 
    jackpot = filter ord3 by ($0 == 'Jackpot');
    jackpot_grp = group jackpot all; 
    cherry = filter ord3 by ($0 == 'Cherry');
    cherry_grp = group cherry all;
    
    jackpots = foreach jackpot_grp {    
        a = foreach jackpot generate $2,$5, $8, $10, $12, $14;
        prep1 = foreach a generate 'Jackpot', 'Jackpot', 'Jackpot', $0..$5, ($0 * $1 * $2) as hits, (int)0 as minus;
        prep2 = foreach prep1 generate $0..$10, ($9 - $10) as adj_hits, (int)1500 as award;
        some1 = foreach prep2 generate $0..$12, ($11 * $12) as out;
    
        p1 = foreach a generate 'Jackpot', 'Jackpot', '-', $0, $1, ($5-$2), $3, $4, $5;
        p2 = foreach p1 generate $0..$8, ($3 * $4 * $5) as hits, (int)0 as minus;
        p3 = foreach p2 generate $0..$10, ($9 - $10) as adj_hits, (int)50 as award;
        some2 = foreach p3 generate $0..$12, ($11 * $12) as out;
        
        pp1 = foreach a generate 'Jackpot', '-', '-', $0, ($4-$1), ($5-$2), $3, $4, $5;
        pp2 = foreach pp1 generate $0..$8, ($3 * $4 * $5) as hits, (int)0 as minus;
        pp3 = foreach pp2 generate $0..$10, ($9 - $10) as adj_hits, (int)5 as award;
        some3 = foreach pp3 generate $0..$12, ($11 * $12) as out;
        
        GENERATE group, some1, some2, some3;
    }  
    
    row1 = foreach jackpots generate  BagToTuple($1);
    row2 = foreach jackpots generate  BagToTuple($2);
    row3 = foreach jackpots generate  BagToTuple($3);
    j_r = union row1, row2, row3;
    jj1 = foreach j_r generate flatten($0);
    --dump jj1;
    
    chs = foreach cherry_grp {
        b = foreach cherry generate $2,$5, $8, $10, $12, $14;
        b1 = foreach b generate 'Cherry', 'Cherry', 'Cherry',  $0..$5, ($0 * $1 * $2) as hits, (int)0 as minus;
        b2 = foreach b1 generate $0..$10, ($9 - $10) as adj_hits, (int)25 as award;
        b3 = foreach b2 generate $0..$12, ($11 * $12) as out;
    
        p1 = foreach b generate 'Cherry', 'Cherry', '-', $0, $1, ($5-$2), $3, $4, $5;
        p2 = foreach p1 generate $0..$8, ($3 * $4 * $5) as hits, (int)0 as minus;
        p3 = foreach p2 generate $0..$10, ($9 - $10) as adj_hits, (int)5 as award;
        some2 = foreach p3 generate $0..$12, ($11 * $12) as out;
        
        pp1 = foreach b generate 'Cherry', '-', '-', $0, ($4-$1), ($5-$2), $3, $4, $5;
        pp2 = foreach pp1 generate $0..$8, ($3 * $4 * $5) as hits, (int)0 as minus;
        pp3 = foreach pp2 generate $0..$10, ($9 - $10) as adj_hits, (int)1 as award;
        some3 = foreach pp3 generate $0..$12, ($11 * $12) as out;
        
        GENERATE group, b3, some2, some3;
    }  
    
    
    ch_row1 = foreach chs generate  BagToTuple($1);
    ch_row2 = foreach chs generate  BagToTuple($2);
    ch_row3 = foreach chs generate  BagToTuple($3);
    ch_r = union ch_row1, ch_row2, ch_row3;
    ch1 = foreach ch_r generate flatten($0);
    --dump ch1;
    
    sev = filter ord3 by ($0 == 'Seven');
    s7 = foreach sev generate 'Seven', 'Seven', 'Seven', $2 as r1, $5 as r2, $8 as r3, $10 as totalR1, $12 as totalR2, $14 as totalR3, ($2*$5*$8) as hits, 0 as minus;
    ss7 = foreach s7 generate $0..$10, ($9-$10) as adj_hits, (int)500 as award; 
    some7 = foreach ss7 generate $0..$12, ($11*$12) as out;

--// end cut
   
    
    bar3 = filter ord3 by ($0 == 'Triple bars');
    sB3 = foreach bar3 generate 'Triple bars', 'Triple bars', 'Triple bars', $2 as r1, $5 as r2, $8 as r3, $10 as totalR1, $12 as totalR2, $14 as totalR3, ($2*$5*$8) as hits, 0 as minus;
    ssB3 = foreach sB3 generate $0..$10, ($9-$10) as adj_hits, (int)90 as award; 
    someB3 = foreach ssB3 generate $0..$12, ($11*$12) as out;
    
    bar1 = filter ord3 by ($0 == 'Single bar');
    sB1 = foreach bar1 generate 'Single bar', 'Single bar', 'Single bar', $2 as r1, $5 as r2, $8 as r3, $10 as totalR1, $12 as totalR2, $14 as totalR3, ($2*$5*$8) as hits, 0 as minus;
    ssB1 = foreach sB1 generate $0..$10, ($9-$10) as adj_hits, (int)30 as award; 
    someB1 = foreach ssB1 generate $0..$12, ($11*$12) as out;
    
    
    bar2 = filter ord3 by ($0 == 'Double bars');
    sb2 = foreach bar2 generate 'Double bars', 'Double bars', 'Double bars', $2 as r1, $5 as r2, $8 as r3, $10 as totalR1, $12 as totalR2, $14 as totalR3, ($2*$5*$8) as hits, 0 as minus;
    sbb2 = foreach sb2 generate $0..$10, ($9-$10) as adj_hits, (int)50 as award; 
    someB2 = foreach sbb2 generate $0..$12, ($11*$12) as out;
   
    preSome = union someB3, someB2, someB1;     
    
    preSome1 = group preSome all;
    preSome2 = foreach preSome1 generate 'Any bars', SUM($1.hits), SUM($1.$3), SUM($1.$4), SUM($1.$5);    
    
    -- (Any bars,14331,51,52,46)
    any = filter ord3 by ($0 == 'Any bars');
    any1_ = foreach any generate 'Any bars', 'Any bars', 'Any bars', $2 as r1, $5 as r2, $8 as r3, $10 as totalR1, $12 as totalR2, $14 as totalR3;
    --, ($2*$5*$8) as hits, 0 as minus;
    any11 = join any1_ by $0, preSome2 by $0;
--store any11 into 'any11';    
--$10 -- sum of hits from b1, b2 and b3 (this is the minus value, double counted in any)
--$11 -- reel1 value of any bars
--$12 -- reel2 value of any bars
--$13 -- reel3 value of any bars 
--(Triple bars,Triple bars,Triple bars,16,17,12,202,158,138,3264,0,3264,90,293760)
--(Single bar,Single bar,Single bar,23,21,17,202,158,138,8211,0,8211,30,246330)
--(Double bars,Double bars,Double bars,12,14,17,202,158,138,2856,0,2856,50,142800)
    any111 = foreach any11 generate $0..$2, $11, $12, $13, $6..$8, ($11 * $12 * $13) as hits, $10;
    --store any111 into 'any111_';
    any12 = foreach any111 generate $0..$10, ($9-$10) as adj_hits, (int)10 as award; 
    
    --dump any12;
   
    someAny = foreach any12 generate $0..$12, ($11*$12) as out;
    --data = union someB1, someB2, someB3, someAny;
    data = union jj1, ch1, some7, someB1, someB2, someB3, someAny;    
    store data into 'data/par/$path';

"""
)
### computes the cycles and totals the total amount been paid out ###
step3 = Pig.compile("""
    data = load 'data/par/$path' using PigStorage() as (s1: chararray, s2: chararray, s3: chararray, r1:long, r2:long, r3:long, r1Total:long,r2Total:long, r3Total:long, hits:long, minus: long, adj_hits: long, award: int, out: long); 
    data1 = foreach data generate *, CurrentTime() as runAs;
    a = group data1 all;
    d = foreach a generate SUM($1.$13) as totalOut;     
    stats1 = cross data1, d;
    s = foreach stats1 generate *;
    stats2 = foreach s generate $0..$15, (long) (data1::r1Total * data1::r2Total * data1::r3Total) as cycles; 
    --dump stats2;
    s2 = foreach stats2 generate *;
    store s2 into 'data/readyToRtp/$path'; 
"""
)

### compute the RTP value ###
### stores the data in ES - RTP value ###
step4 = Pig.compile("""
    REGISTER '../../../lib/elasticsearch-hadoop-1.3.0.BUILD-SNAPSHOT.jar';
    define ESStorage org.elasticsearch.hadoop.pig.ESStorage('es.resource=rtp-sample-par/$path');
    
    data = load 'data/readyToRtp/$path' using PigStorage() as (s1: chararray, s2: chararray, s3: chararray, r1:long, r2:long, r3:long, r1Total:long,r2Total:long, r3Total:long, hits:long, minus: long, adj_hits: long, award: int, out: long, ranDate:chararray, totalOut: long, totalCycles:long); 
    data1 = foreach data generate $0..$16, (double)(100*((double)$15/(double)$16)) as rtp;
    store data1 into 'rtp-sample-par/$path' USING ESStorage('es.http.timeout = 5m      es.index.auto.create = false'); 
"""
)
params = {'file':'data', 'path':'path', 'format':'format', 'skip':'true'}
filename = open('data/fileList2.txt', 'rU')
# boundF = Prep.bind();
#     job2 = boundF.runSingle();
#     if not job2.isSuccessful():
#         raise 'failed'
        
for r in filename.readlines():
    file = r.split('\t')[0]
    print "*****************************" + file;
    params["file"]=file
    
    path = r.split('\t')[1]
    print "*****************************" + path;
    params["path"]=path
    
    format = r.split('\t')[2]
    print "*****************************" + format;
    params["format"]=format
    
    skip = r.split('\t')[3]
    print "*****************************" + skip;
    params["skip"]=skip
    
    if (not skip):
        bound1 = Prep.bind(params)
        bound1.runSingle() 
        
        bound2 = step1.bind(params)
        bound2.runSingle() 
    
    
    bound3 = step2.bind(params)
    bound3.runSingle() 
    
    bound4 = step3.bind(params)
    bound4.runSingle() 
    
    bound5 = step4.bind(params)
    bound5.runSingle() 

    
filename.close

if __name__ == '__main__':
    main()