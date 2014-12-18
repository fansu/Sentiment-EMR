a_t1 = load '*.txt';
a_t2 = load '*.txt';
a_t3 = load '*.txt';
a_t4 = load '*.txt';
dic = load '*.txt';

b_t1 = foreach a_t1 generate flatten(TOKENIZE((chararray)$0)) as word;
c_t1 = group b_t1 by word;
d_t1 = foreach c_t1 generate group, COUNT(b_t1);
e_t1 = order d_t1 by $1 DESC;
f_t1 = join e_t1 by $0, dic by $0;
g_t1 = foreach f_t1 generate $0, $1;
h_t1 = order g_t1 by $1 desc;

b_t2 = foreach a_t2 generate flatten(TOKENIZE((chararray)$0)) as word;
c_t2 = group b_t2 by word;
d_t2 = foreach c_t2 generate group, COUNT(b_t2);
e_t2 = order d_t2 by $1 DESC;
f_t2 = join e_t2 by $0, dic by $0;
g_t2 = foreach f_t2 generate $0, $1;
h_t2 = order g_t2 by $1 desc;

b_t3 = foreach a_t3 generate flatten(TOKENIZE((chararray)$0)) as word;
c_t3 = group b_t3 by word;
d_t3 = foreach c_t3 generate group, COUNT(b_t3);
e_t3 = order d_t3 by $1 DESC;
f_t3 = join e_t3 by $0, dic by $0;
g_t3 = foreach f_t3 generate $0, $1;
h_t3 = order g_t3 by $1 desc;

b_t4 = foreach a_t4 generate flatten(TOKENIZE((chararray)$0)) as word;
c_t4 = group b_t4 by word;
d_t4 = foreach c_t4 generate group, COUNT(b_t4);
e_t4 = order d_t4 by $1 DESC;
f_t4 = join e_t4 by $0, dic by $0;
g_t4 = foreach f_t4 generate $0, $1;
h_t4 = order g_t4 by $1 desc;

o_1 = join h_t1 by $0, h_t2 by $0;
o_12 = foreach o_1 generate $0, $1, $3;
o_23 = join o_12 by $0, h_t3 by $0;
o_123 = foreach o_23 generate $0, $1, $2, $4;
o_4 = join o_123 by $0, h_t4 by $0;
o = foreach o_4 generate $0, $1, $2, $3, $5;
store o into '*.txt';
