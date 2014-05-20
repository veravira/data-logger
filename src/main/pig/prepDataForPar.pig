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
dump preSome2;

any = filter ord3 by ($0 == 'Any bars');
any1_ = foreach any generate 'Any bars', 'Any bars', 'Any bars', $2 as r1, $5 as r2, $8 as r3, $10 as totalR1, $12 as totalR2, $14 as totalR3, ($2*$5*$8) as hits, 0 as minus;
any11 = join any1_ by $0, preSome2 by $0;
any111 = foreach any11 generate $0..$9, $12;
any11_ = foreach any111 generate $0..$10, ($9-$10) as adj_hits, (int)10 as award; 
someAny = foreach any11_ generate $0..$12, ($11*$12) as out;

data = union jj1, ch1, some7, someB1, someB2, someB3, someAny;
dump data; 
store data into 'par';

