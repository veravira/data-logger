#!/usr/bin/python
from org.apache.pig.scripting import *
from subprocess import call
def main():
    
    output = "output/data_0"
    ### this script uses raw data already pre parsed to do just the analytics and save the final results to ES (the actual RTP)###
    ### this is an utility to transfer resulst to ES after script test2.pig has been ran and raw data is saved in ES already###
###  (Seven,1,10,Seven,2,2,Seven,3,22,1,223,2,237,3,233)  ###
step2 = Pig.compile("""
    c = load '$file' using PigStorage() as (symbol: chararray, reelNumber:int, total:long);
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
    /**
    same of the dump below
    $10 -- r1Total, $12 -- r2Total, $14 -- r3Total
    (Blank,1,100,Blank,2,94,Blank,3,96,1,223,2,237,3,233)
    (Seven,1,10,Seven,2,2,Seven,3,22,1,223,2,237,3,233)
    (Cherry,1,26,Cherry,2,24,Cherry,3,12,1,223,2,237,3,233)
    (Jackpot,1,7,Jackpot,2,9,Jackpot,3,3,1,223,2,237,3,233)
    (Any bars,1,40,Any bars,2,54,Any bars,3,50,1,223,2,237,3,233)
    (Single bar,1,15,Single bar,2,23,Single bar,3,15,1,223,2,237,3,233)
    (Double bars,1,6,Double bars,2,11,Double bars,3,12,1,223,2,237,3,233)
    (Triple bars,1,19,Triple bars,2,20,Triple bars,3,23,1,223,2,237,3,233)
    **/
    
    --- ---


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
    dump preSome;
    preSome1 = group preSome all;
    preSome2 = foreach preSome1 generate 'Any bars', SUM($1.hits);    
    
    any = filter ord3 by ($0 == 'Any bars');
    any1_ = foreach any generate 'Any bars', 'Any bars', 'Any bars', $2 as r1, $5 as r2, $8 as r3, $10 as totalR1, $12 as totalR2, $14 as totalR3, ($2*$5*$8) as hits, 0 as minus;
    any11 = join any1_ by $0, preSome2 by $0;
    any111 = foreach any11 generate $0..$10, $12;
    any11_ = foreach any111 generate $0..$9,$11, ($9-$11) as adj_hits, (int)10 as award; 
    someAny = foreach any11_ generate $0..$12, ($11*$12) as out;
    
    data = union jj1, ch1, some7, someB1, someB2, someB3, someAny;    
    store data into 'par/$temp';

"""
)


### computes the cycles and totals the total amount been paid out ###
step3 = Pig.compile("""
    data = load 'par/$temp' using PigStorage() as (s1: chararray, s2: chararray, s3: chararray, r1:long, r2:long, r3:long, r1Total:long,r2Total:long, r3Total:long, hits:long, minus: long, adj_hits: long, award: int, out: long); 
    data1 = foreach data generate *, CurrentTime() as runAs;
    a = group data1 all;
    d = foreach a generate SUM($1.$13) as totalOut;     
    stats1 = cross data1, d;
    s = foreach stats1 generate *;
    stats2 = foreach s generate $0..$15, (long) (data1::r1Total * data1::r2Total * data1::r3Total) as cycles; 
    --dump stats2;
    s2 = foreach stats2 generate *;
    store s2 into 'readyToRtp/$temp'; 
"""
)

### compute the RTP value ###
### stores the data in ES - RTP value ###
step4 = Pig.compile("""
    REGISTER '../../../lib/elasticsearch-hadoop-1.3.0.BUILD-SNAPSHOT.jar';
    define ESStorage org.elasticsearch.hadoop.pig.ESStorage('es.resource=$index');
    
    data = load 'readyToRtp/$temp' using PigStorage() as (s1: chararray, s2: chararray, s3: chararray, r1:long, r2:long, r3:long, r1Total:long,r2Total:long, r3Total:long, hits:long, minus: long, adj_hits: long, award: int, out: long, ranDate:chararray, totalOut: long, totalCycles:long); 
    data1 = foreach data generate $0..$16, (double)(100*((double)$15/(double)$16)) as rtp;
    store data1 into '$index' USING ESStorage('es.http.timeout = 5m      es.index.auto.create = false'); 
"""
)
params = {'file':'data', 'path':'path', 'format':'format', 'index':'index', 'temp':'temp'}
file = open('data/fileList1.txt', 'rU')
r = file.readline()
i = 0
# boundF = Prep.bind();
#     job2 = boundF.runSingle();
#     if not job2.isSuccessful():
#         raise 'failed'
        
while (r is not None):
    location = r.split('\t')[0]
    ind = r.split('\t')[1]
    tmp = r.split('\t')[2]
    print "*****************************" + location;
    params["file"]=location
    params["index"]=ind
    params["temp"]=tmp
    
    bound3 = step2.bind(params)
    step2 = bound3.runSingle() 
    
    bound4 = step3.bind(params)
    step3 = bound4.runSingle() 
    
    bound5 = step4.bind(params)
    step4 = bound5.runSingle() 

file.close

if __name__ == '__main__':
    main()