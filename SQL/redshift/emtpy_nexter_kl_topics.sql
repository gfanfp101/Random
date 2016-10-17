drop table if exists tmp_ads_all_user_topic_percent;
with cnt_table as (select count(distinct uid) as cnt from tmp_ads_user_2016_interest_cluster_tag)
select t.title, cast(count(distinct t.uid) as float)/c.cnt*1000000 as p
into tmp_ads_all_user_topic_percent
from tmp_ads_user_2016_interest_cluster_tag t
join cnt_table c
on 1=1
group by t.title, c.cnt;

drop table if exists tmp_ads_empty_nexter_topic_percent;
with cnt_table as (select count(distinct uid) as cnt from tmp_ads_user_empty_nexter_topics)
select t.title, cast(count(t.uid) as float)/c.cnt*1000000 as p
into tmp_ads_empty_nexter_topic_percent
from tmp_ads_user_empty_nexter_topics t
join cnt_table c
on 1=1
group by t.title, c.cnt;

drop table if exists tmp_empty_nexter_topic_kl_score;
select a.title, e.p*log(e.p/a.p) as score
into tmp_empty_nexter_topic_kl_score
from tmp_ads_all_user_topic_percent a
inner join tmp_ads_empty_nexter_topic_percent e
on a.title = e.title;

select a.title, a.score, b.p, a.score - b.p as d from
(select title, score/(select max(score) from tmp_empty_nexter_topic_kl_score) as score from tmp_empty_nexter_topic_kl_score) a
left outer join
(select title, p/(select max(p) from tmp_ads_all_user_topic_percent) as p from tmp_ads_all_user_topic_percent) b
on a.title = b.title
order by d desc
limit 500

select title from tmp_empty_nexter_topic_kl_score order by score desc limit 100;

