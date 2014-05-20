data = load 'correctedRtp/data/r2.log' using PigStorage(',') as (r2:int, s2:chararray, yoffset: double); 
    data1 = foreach data generate CurrentTime() as runAs, $0, $1, $2;
store data1 into 'r2_withTime';
