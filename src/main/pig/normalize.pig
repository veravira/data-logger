--pig -f normalize.pig -param factor='-1'
define createPaytable(symbols)
returns award {
    data1 =  filter data by '$reelNum'=='$reelSymbol';
    --dump cherry1;
    data11 = foreach (group data1 all) generate '$reelSymbol', COUNT(data1) as sym1;
    
 	$award = foreach data11 generate *;
};
	REGISTER '../../../target/pig-template-1.0-SNAPSHOT.jar';

	DEFINE paytable com.gamblitgaming.pig.PaytableValuesUDF;

 c = load '$src' using PigStorage() as (symbol: chararray, reelNumber:int, total:long);
 	A = foreach c generate $0, $1, ((long)($2/10) == 0l?$2:(long)($2/10));
    dupC = foreach A generate *;
 
    cc1 = join c by $0, dupC by $0;
    --noSelf = filter cc1 by ($1<$4);
    --ord1 = filter noSelf by ($1<$4); 
    ord1 = filter cc1 by ($1<$4); 
    noSelf1 = join ord1 by $0, c by $0;
    ord2 = filter noSelf1 by ($4 < $7 AND $7<$1);

--    ord2 = filter noSelf2 by ($4<$7);
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
        --prep2 = foreach prep1 generate $0..$10, ($9 - $10) as adj_hits, (int)1500 as award;
        prep2 = foreach prep1 generate $0..$10, ($9 - $10) as adj_hits, paytable('jjj', $factor) as award;
        some1 = foreach prep2 generate $0..$12, ($11 * $12) as out;
    
        p1 = foreach a generate 'Jackpot', 'Jackpot', '-', $0, $1, ($5-$2), $3, $4, $5;
        p2 = foreach p1 generate $0..$8, ($3 * $4 * $5) as hits, (int)0 as minus;
        p3 = foreach p2 generate $0..$10, ($9 - $10) as adj_hits, paytable('jj-', $factor) as award;
        some2 = foreach p3 generate $0..$12, ($11 * $12) as out;
        
        pp1 = foreach a generate 'Jackpot', '-', '-', $0, ($4-$1), ($5-$2), $3, $4, $5;
        pp2 = foreach pp1 generate $0..$8, ($3 * $4 * $5) as hits, (int)0 as minus;
        pp3 = foreach pp2 generate $0..$10, ($9 - $10) as adj_hits, paytable('j--', $factor) as award;
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
        -- b2 = foreach b1 generate $0..$10, ($9 - $10) as adj_hits, (int)25 as award;
        b2 = foreach b1 generate $0..$10, ($9 - $10) as adj_hits, paytable('ccc', $factor) as award;
        b3 = foreach b2 generate $0..$12, ($11 * $12) as out;
    
        p1 = foreach b generate 'Cherry', 'Cherry', '-', $0, $1, ($5-$2), $3, $4, $5;
        p2 = foreach p1 generate $0..$8, ($3 * $4 * $5) as hits, (int)0 as minus;
        p3 = foreach p2 generate $0..$10, ($9 - $10) as adj_hits, paytable('cc-', $factor) as award;
        some2 = foreach p3 generate $0..$12, ($11 * $12) as out;
        
        pp1 = foreach b generate 'Cherry', '-', '-', $0, ($4-$1), ($5-$2), $3, $4, $5;
        pp2 = foreach pp1 generate $0..$8, ($3 * $4 * $5) as hits, (int)0 as minus;
        pp3 = foreach pp2 generate $0..$10, ($9 - $10) as adj_hits, paytable('c--', $factor) as award;
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
    ss7 = foreach s7 generate $0..$10, ($9-$10) as adj_hits, paytable('777', $factor) as award; 
    some7 = foreach ss7 generate $0..$12, ($11*$12) as out;

--// end cut
   
    
    bar3 = filter ord3 by ($0 == 'Triple bars');
    sB3 = foreach bar3 generate 'Triple bars', 'Triple bars', 'Triple bars', $2 as r1, $5 as r2, $8 as r3, $10 as totalR1, $12 as totalR2, $14 as totalR3, ($2*$5*$8) as hits, 0 as minus;
    ssB3 = foreach sB3 generate $0..$10, ($9-$10) as adj_hits, paytable('b3b3b3', $factor) as award; 
    someB3 = foreach ssB3 generate $0..$12, ($11*$12) as out;
    
    bar1 = filter ord3 by ($0 == 'Single bar');
    sB1 = foreach bar1 generate 'Single bar', 'Single bar', 'Single bar', $2 as r1, $5 as r2, $8 as r3, $10 as totalR1, $12 as totalR2, $14 as totalR3, ($2*$5*$8) as hits, 0 as minus;
    ssB1 = foreach sB1 generate $0..$10, ($9-$10) as adj_hits, paytable('b1b1b1', $factor) as award; 
    someB1 = foreach ssB1 generate $0..$12, ($11*$12) as out;
    
    
    bar2 = filter ord3 by ($0 == 'Double bars');
    sb2 = foreach bar2 generate 'Double bars', 'Double bars', 'Double bars', $2 as r1, $5 as r2, $8 as r3, $10 as totalR1, $12 as totalR2, $14 as totalR3, ($2*$5*$8) as hits, 0 as minus;
    sbb2 = foreach sb2 generate $0..$10, ($9-$10) as adj_hits, paytable('b2b2b2', $factor) as award; 
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
    any12 = foreach any111 generate $0..$10, ($9-$10) as adj_hits, paytable('ababab', $factor) as award; 
    
    --dump any12;
   
    someAny = foreach any12 generate $0..$12, ($11*$12) as out;
    --data = union someB1, someB2, someB3, someAny;
	data = union jj1, ch1, some7, someB1, someB2, someB3, someAny;    
    
    store data into '$dest';