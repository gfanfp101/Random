drop table if exists tmp_topic_global_avg_score;
select title, displayname, avg(log(score)) as avg_score into tmp_topic_global_avg_score
from topics
where score > 350
group by title, displayname;

drop table if exists tmp_topic_group_avg_score;
select g.group_title, g.group_displayname, t.one_title, t.one_displayname, avg(log(t.one_score)) as avg_score
into tmp_topic_group_avg_score
from
(select uid, title as group_title, displayname as group_displayname
from (select *, row_number() over (partition by uid order by score desc) as rank
from (select * from topics where title not like 's:%'))
where rank <= 100 and score > 350
group by uid) g
join
(select uid, title as one_title, displayname as one_displayname, score as one_score
from (select *, row_number() over (partition by uid order by score desc) as rank
from (select * from topics where title not like 's:%'))
where rank <= 100 and score > 350
group by uid) t
on g.uid = t.uid
where g.group_title <> t.one_title and g.group_displayname <> t.one_displayname
group by g.group_title, g.group_displayname, t.one_title, t.one_displayname;

drop table if exists tmp_topic_kl_score;
select a.title, e.p*log(e.p/a.p) as score
into tmp_topic_kl_score
from tmp_topic_global_avg_score g
join tmp_topic_group_avg_score t
on g.title = t.group_title and g.displayname = t.group_displayname;






