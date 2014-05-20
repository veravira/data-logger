
/** 
* this portion of the script loads data of the following format 
*{(Single bar,-2200),(Blank,-100),(Cherry,-800)})
*({(Blank,-300),(Seven,-1600),(Triple bars,-1800)})
*({(Blank,-700),(Blank,-500),(Blank,-1500)})
* and dumps to the screen the actual reel tuple
**/
a = LOAD 'd1' USING JsonLoader('reels:{(symbol:chararray,yoffset:int)}');
DESCRIBE a;
a10 = limit a 10;
b = foreach a10 generate $0.$0;
describe b;
--store b into 'b';
/**
*{(Blank),(Cherry),(Triple bars)}
*{(Blank),(Double bars),(Blank)}
*{(Blank),(Triple bars),(Blank)}
**/
tt = load 'b' using PigStorage(',') as (reel1:chararray, reel2:chararray, reel3:chararray);
t1 = foreach tt generate $0, $1, $2;
dump t1;  
store t1 into 't1';
--c = foreach b generate flatten($0);
--DUMP c;
