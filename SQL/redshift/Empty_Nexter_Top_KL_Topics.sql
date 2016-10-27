select title as one_title, displayname as one_displayname, count(distinct uid) as cnt
into tmp_topic_empty_nexter_cnt
from (select *, row_number() over (partition by uid order by score desc) as rank
from (select * from topics where title not like 's:%' and 
uid in (select uid from tmp_empty_nexter_age where empty_nexter = 'E' and child_under_13 = 'U')))
where rank <= 50 and score > 350
group by one_title, one_displayname;

drop table if exists tmp_topic_empty_nexter_kl_cnt;
select 
g.group_title, g.group_displayname, g.cnt as global_cnt, t.cnt as group_cnt, 
cast(t.cnt as float)/(select count(distinct uid) from tmp_empty_nexter_age)*log(cast(t.cnt as float)*(select count(distinct uid) from topics)/cast(g.cnt as float)/(select count(distinct uid) from tmp_empty_nexter_age)) as kl_cnt
into tmp_topic_empty_nexter_kl_cnt
from 
tmp_topic_global_cnt g
join
tmp_topic_empty_nexter_cnt t
on
g.group_title = t.one_title and g.group_displayname = t.one_displayname;

select group_displayname from tmp_topic_empty_nexter_kl_cnt where group_cnt > 3500 order by kl_cnt desc limit 500

select avg(group_cnt), min(group_cnt), max(group_cnt) from tmp_topic_empty_nexter_kl_cnt
