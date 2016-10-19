drop table if exists tmp_topic_global_cnt;
select title as group_title, displayname as group_displayname, count(distinct uid) as cnt
into tmp_topic_global_cnt
from (select *, row_number() over (partition by uid order by score desc) as rank
from (select * from topics where title not like 's:%'))
where rank <= 20 and score > 350
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
