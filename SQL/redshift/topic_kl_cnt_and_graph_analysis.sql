drop table if exists tmp_topic_global_cnt;
select title as group_title, displayname as group_displayname, count(distinct uid) as cnt
into tmp_topic_global_cnt
from (select *, row_number() over (partition by uid order by score desc) as rank
from (select * from topics where title not like 's:%'))
where rank <= 50 and score > 350
group by group_title, group_displayname;

drop table if exists tmp_topic_group_cnt;
select g.group_title, g.group_displayname, t.one_title, t.one_displayname, count(distinct t.uid) as cnt
into tmp_topic_group_cnt
from
(select uid, title as group_title, displayname as group_displayname
from (select *, row_number() over (partition by uid order by score desc) as rank
from (select * from topics where title not like 's:%'))
where rank <= 50 and score > 350) g
join
(select uid, title as one_title, displayname as one_displayname, score as one_score
from (select *, row_number() over (partition by uid order by score desc) as rank
from (select * from topics where title not like 's:%'))
where rank <= 50 and score > 350) t
on g.uid = t.uid
where g.group_title <> t.one_title and g.group_displayname <> t.one_displayname
group by g.group_title, g.group_displayname, t.one_title, t.one_displayname;

drop table if exists tmp_topic_kl_cnt;
select 
g.group_title, g.group_displayname, t.one_title, t.one_displayname, g.cnt as global_cnt, t.cnt as group_cnt, 
cast(t.cnt as float)/g.cnt*log(cast(t.cnt as float)*(select count(distinct uid) from topics)/pow(g.cnt, 2)) as kl_cnt
into tmp_topic_kl_cnt
from 
tmp_topic_global_cnt g
join
tmp_topic_group_cnt t
on
g.group_title = t.group_title and g.group_displayname = t.group_displayname;

select * from tmp_topic_kl_cnt where group_title = 'personalfinance' order by group_cnt desc, kl_cnt desc, global_cnt desc limit 300

drop table if exists tmp_topic_kl_cnt_filtered_v0;
create table tmp_topic_kl_cnt_filtered_v0 as
select *, 0 as rank, 0 as level from tmp_topic_kl_cnt where group_title = 'personalfinance' and group_cnt > 5000 order by kl_cnt desc limit 64

drop table if exists tmp_topic_kl_cnt_filtered_v1;
create table tmp_topic_kl_cnt_filtered_v1 as
select *, 1 as level from 
(select group_title, group_displayname, one_title, one_displayname, global_cnt, group_cnt, kl_cnt, row_number() over (partition by group_title, group_displayname order by kl_cnt desc) as rank
from tmp_topic_kl_cnt where concat(group_title, group_displayname) in 
(select concat(one_title, one_displayname) from tmp_topic_kl_cnt_filtered_v0))
where group_cnt > 5000 and rank <= 32;

drop table if exists tmp_topic_kl_cnt_filtered_v2;
create table tmp_topic_kl_cnt_filtered_v2 as
select *, 2 as level from 
(select group_title, group_displayname, one_title, one_displayname, global_cnt, group_cnt, kl_cnt, row_number() over (partition by group_title, group_displayname order by kl_cnt desc) as rank
from tmp_topic_kl_cnt where concat(group_title, group_displayname) in 
(select concat(one_title, one_displayname) as topic_tag from tmp_topic_kl_cnt_filtered_v1))
where group_cnt > 5000 and rank <= 16;

drop table if exists tmp_topic_kl_cnt_filtered_v3;
create table tmp_topic_kl_cnt_filtered_v3 as
select * from tmp_topic_kl_cnt_filtered_v0
union all
select * from tmp_topic_kl_cnt_filtered_v1
union all
select * from tmp_topic_kl_cnt_filtered_v2;

select max(kl_cnt), min(kl_cnt), avg(kl_cnt), cast(stddev(kl_cnt) as dec(18,2)), count(distinct group_title), count(distinct one_title) 
from tmp_topic_kl_cnt_filtered_v3

select *
from tmp_topic_kl_cnt a
where group_title in ('personalfinance') and group_cnt > 5000
order by kl_cnt desc
limit 200
