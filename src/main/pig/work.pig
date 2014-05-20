	REGISTER '../elasticsearch-hadoop-1.3.0.BUILD-SNAPSHOT.jar';
	define ESStorage org.elasticsearch.hadoop.pig.ESStorage('es.resource=outcome-1gb/all, es.mapping.names=date:@timestamp');
 	
	
	/** data gets store first from parse.pig and the job is done here **/
	b3 = load 'sample' using PigStorage('\t') as (r1:chararray, r2:chararray, r3:chararray, ct: int,cw: int,cwon: int,status: chararray,dt: datetime);
	store b3 into 'outcome-1gb/all' USING ESStorage('es.http.timeout = 5m 	 es.index.auto.create = false');	

	B = foreach b3 generate $0, $1, $2, $3, $4, $5, $6,$7;
	noblanks = filter B by ($0 != 'Blank' AND $1 != 'Blank' AND $2 != 'Blank');

	noblks_grp = group noblanks by ($0, $1, $2);
	noblanks1 = foreach noblks_grp generate group, COUNT($1);
	store noblanks1 into 'gb1/noblanks1';

	b4 = group B by ($0, $1,$2);
	b5 = foreach b4 generate group, COUNT($1), flatten($1.$5);
	bb5 = distinct b5; 
	
	r1 = group B by $0;
	rr1 = foreach r1 generate group, COUNT($1);
	
	j1 = filter rr1 by ($0 == 'Jackpot');
	j1_grp = group j1 ALL;
	j1_total = foreach j1_grp generate $0, COUNT(j1), 'Jackpot';
	
	c1 = filter rr1 by ($0 == 'Cherry');
	c1_grp = group c1 ALL;
	c1_total = foreach c1_grp generate $0, COUNT(c1), 'Cherry';
	
	sb1 = filter rr1 by ($0 == 'Single bar');
	sb1_grp = group sb1 ALL;
	sb1_total = foreach sb1_grp generate $0, COUNT(sb1), 'Single bar';
	
	db1 = filter rr1 by ($0 == 'Double bars');
	db1_grp = group db1 ALL;
	db1_total = foreach db1_grp generate $0, COUNT(db1), 'Double bar';
	
	ab1 = filter rr1 by (($0 == 'Triple bars') OR ($0 == 'Single bar') OR ($0 == 'Double bars'));
	ab1_grp = group ab1 ALL;
	ab1_total = foreach ab1_grp generate $0, COUNT(ab1), 'Any bar';
	
	tb1 = filter rr1 by ($0 == 'Triple bars');
	tb1_grp = group tb1 ALL;
	tb1_total = foreach tb1_grp generate $0, COUNT(tb1), 'Triple bar';
	
	blk1 = filter rr1 by ($0 == 'Blank');
	blk1_grp = group blk1 ALL;
	blk1_total = foreach blk1_grp generate $0, COUNT(blk1), 'Blank';
	
	s1 = filter rr1 by ($0 == 'Seven');
	s1_grp = group s1 ALL;
	s1_total = foreach s1_grp generate $0, COUNT(s1), '7';
	
	rrr1 = union j1_total, c1_total, sb1_total, db1_total,ab1_total, tb1_total, blk1_total, s1_total;
	store rrr1 into 'gb1/stats-r1';
	

	j2 = filter B by ($1 == 'Jackpot');
	j_grp = group j2 ALL;
	j2_total = foreach j_grp generate $0, COUNT(j2), 'Jackpot';
	dump j2_total;

	c2 = filter B by ($1 == 'Cherry');
	c_grp = group c2 ALL;
	c2_total = foreach c_grp generate $0, COUNT(c2), 'Cherry';
	dump c2_total;

	sb2 = filter B by ($1 == 'Single bar');
	sb_grp = group sb2 ALL;
	sb2_total = foreach sb_grp generate $0, COUNT(sb2), 'Single bar';
	dump sb2_total;

	db2 = filter B by ($1 == 'Double bars');
	db_grp = group db2 ALL;
	db2_total = foreach db_grp generate $0, COUNT(db2), 'Double bars';
	dump db2_total;

	ab2 = filter B by (($1 == 'Triple bars') OR ($1 == 'Single bar') OR ($1 == 'Double bars'));
	ab_grp = group ab2 ALL;
	ab2_total = foreach ab_grp generate $0, COUNT(ab2), 'Any Bar';
	dump ab2_total;

	tb2 = filter B by ($1== 'Triple bars');
	tb_grp = group tb2 ALL;
	tb2_total = foreach tb_grp generate $0, COUNT(tb2), 'Triple bars';
	dump tb2_total;

	blk2 = filter B by ($1 == 'Blank');
	blk_grp = group blk2 ALL;
	blk2_total = foreach blk_grp generate $0, COUNT(blk2), 'Blank';
	dump blk2_total;

	s2 = filter B by ($1 == 'Seven');
	s_grp = group s2 ALL;
	s2_total = foreach s_grp generate $0, COUNT(s2), '7';
	dump s2_total;

	rrr2 = union j2_total, c2_total, sb2_total, db2_total,ab2_total, tb2_total, blk2_total, s2_total;

	store rrr2 into 'gb1/stats-r2';


/** thrid reel counts for precentage distribution**/
j3 = filter B by ($2 == 'Jackpot');
	j3_grp = group j3 ALL;
	j3_total = foreach j3_grp generate $0, COUNT(j3), 'Jackpot';
	dump j3_total;

	c3 = filter B by ($2 == 'Cherry');
	c3_grp = group c3 ALL;
	c3_total = foreach c3_grp generate $0, COUNT(c3), 'Cherry';
	dump c3_total;

	sb3 = filter B by ($2 == 'Single bar');
	sb3_grp = group sb3 ALL;
	sb3_total = foreach sb3_grp generate $0, COUNT(sb3), 'Single bar';
	dump sb3_total;

	db3 = filter B by ($2 == 'Double bars');
	db3_grp = group db3 ALL;
	db3_total = foreach db3_grp generate $0, COUNT(db3), 'Double bars';
	dump db3_total;

	ab3 = filter B by (($2 == 'Triple bars') OR ($2 == 'Single bar') OR ($2 == 'Double bars'));
	ab3_grp = group ab3 ALL;
	ab3_total = foreach ab3_grp generate $0, COUNT(ab3), 'Any Bar';
	dump ab3_total;

	tb3 = filter B by ($2== 'Triple bars');
	tb3_grp = group tb3 ALL;
	tb3_total = foreach tb3_grp generate $0, COUNT(tb3), 'Triple bars';
	dump tb3_total;

	blk3 = filter B by ($2 == 'Blank');
	blk3_grp = group blk3 ALL;
	blk3_total = foreach blk3_grp generate $0, COUNT(blk3), 'Blank';
	dump blk3_total;

	s3 = filter B by ($2 == 'Seven');
	s3_grp = group s3 ALL;
	s3_total = foreach s3_grp generate $0, COUNT(s3), '7';
	dump s3_total;
	
	rrr3 = union j3_total, c3_total, sb3_total, db3_total,ab3_total, tb3_total, blk3_total, s3_total;
	store rrr3 into 'gb1/stats-r3';
	--STORE totalch12 into 'totalch12';
	--store ch12 into 'outcome-ch12/cherry12' USING ESStorage('es.http.timeout = 5m 	 es.index.auto.create = false');

/**
	b7 = foreach b4 generate group, flatten($1.$6);
	bb7 = distinct b7;
	
	g1 = join bb5 by $0, bb7 by $0;

	g = foreach g1 generate $0, $1, $2, $4;

	dump g;

	store g into 'g';

	wins = filter g by $3=='WIN';
**/	
	/**
	dump wins;
	store wins into 'wins';
	**/
	