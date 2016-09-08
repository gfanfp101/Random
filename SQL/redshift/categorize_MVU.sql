drop table if exists tmp_ads_publication_category;

create table tmp_ads_publication_category(
	  category_id int,
    category_name varchar encode lzo,
    partner_id varchar encode lzo,
    partner_name varchar encode lzo,
    default_feed varchar encode lzo
)

COPY tmp_ads_publication_category FROM 's3://..../tmp/gfan/ads_publication_category.csv' CREDENTIALS 'aws_access_key_id=...' delimiter ',' emptyasnull blanksasnull maxerror 2000 ignoreblanklines TRIMBLANKS TRUNCATECOLUMNS;

select 
b.category_id, b.category_name, count(distinct c.uid) as user_count
from 
toc a
join
tmp_ads_user_2016_all c
on a.uid = c.uid
join
tmp_ads_publication_category b
on a.section_id = b.default_feed
group by b.category_id, b.category_name

create table tmp_ads_cluster_topics_50(
    cluster_tag varchar encode lzo,
    topic varchar encode lzo
)

create table tmp_ads_cluster_topics_100(
    cluster_tag varchar encode lzo,
    topic varchar encode lzo
)

COPY tmp_ads_cluster_topics_100 FROM 's3://.../tmp/gfan/topic_affinity_100.tsv' CREDENTIALS 'aws_access_key_id=...' delimiter '\t' emptyasnull blanksasnull maxerror 2000 ignoreblanklines TRIMBLANKS TRUNCATECOLUMNS;

select * from tmp_ads_cluster_topics100 limit 100;

select c.cluster_tag, count(distinct a.uid) as user_count, max(b.score) as max_score, min(b.score) as min_score, avg(b.score) as avg_score, cast(stddev(b.score) as dec(18,2)) as std_dev, tc.topics as topics
from
(select uid from tmp_ads_user_2016_all) a
join
topics b
on a.uid = b.uid
join
tmp_ads_cluster_topics_100 c
on b.title = c.topic
join
(select cluster_tag, listagg(topic, ',') as topics from tmp_ads_cluster_topics_100 group by cluster_tag) as tc
on c.cluster_tag = tc.cluster_tag
group by c.cluster_tag, tc.topics

select count(h.uid), h.topic_cnt_bucket from
(
select k.uid, 
case 
when k.cnt > 0 and k.cnt <= 10 then 'a:0-10' 
when k.cnt > 10 and k.cnt <= 20 then 'b:10-20' 
when k.cnt > 20 and k.cnt <= 30 then 'c:20-30' 
when k.cnt > 30 and k.cnt <= 40 then 'd:30-40' 
when k.cnt > 40 and k.cnt <= 50 then 'e:40-50' 
when k.cnt > 50 and k.cnt <= 100 then 'f:50-100' 
when k.cnt > 100 then 'g:100+' 
end 
as topic_cnt_bucket
from (
	select uid, count(*) as cnt from topics group by uid
) k 
) h group by h.topic_cnt_bucket 

select count(h.uid), h.toc_cnt_bucket from
(
select k.uid, 
case 
when k.cnt > 0 and k.cnt <= 10 then 'a:0-10' 
when k.cnt > 10 and k.cnt <= 20 then 'b:10-20' 
when k.cnt > 20 and k.cnt <= 30 then 'c:20-30' 
when k.cnt > 30 and k.cnt <= 40 then 'd:30-40' 
when k.cnt > 40 and k.cnt <= 50 then 'e:40-50' 
when k.cnt > 50 and k.cnt <= 100 then 'f:50-100' 
when k.cnt > 100 then 'g:100+' 
end 
as toc_cnt_bucket
from (
	select uid, count(*) as cnt from toc group by uid
) k 
) h group by h.toc_cnt_bucket 

select count(h.uid), h.topic_cnt_bucket from
(
select k.uid, 
case 
when k.cnt > 0 and k.cnt <= 10 then 'a:0-10' 
when k.cnt > 10 and k.cnt <= 20 then 'b:10-20' 
when k.cnt > 20 and k.cnt <= 30 then 'c:20-30' 
when k.cnt > 30 and k.cnt <= 40 then 'd:30-40' 
when k.cnt > 40 and k.cnt <= 50 then 'e:40-50' 
when k.cnt > 50 and k.cnt <= 100 then 'f:50-100' 
when k.cnt > 100 then 'g:100+' 
end 
as topic_cnt_bucket
from (
	select a.uid, a.cnt from 
	( select uid, count(*) as cnt from topics group by uid) a
	join 
	( select uid from tmp_ads_user_2016_all) b
	on a.uid = b.uid
) k 
) h group by h.topic_cnt_bucket 

select count(h.uid), h.toc_cnt_bucket from
(
select k.uid, 
case 
when k.cnt > 0 and k.cnt <= 10 then 'a:0-10' 
when k.cnt > 10 and k.cnt <= 20 then 'b:10-20' 
when k.cnt > 20 and k.cnt <= 30 then 'c:20-30' 
when k.cnt > 30 and k.cnt <= 40 then 'd:30-40' 
when k.cnt > 40 and k.cnt <= 50 then 'e:40-50' 
when k.cnt > 50 and k.cnt <= 100 then 'f:50-100' 
when k.cnt > 100 then 'g:100+' 
end 
as toc_cnt_bucket
from (
	select a.uid, a.cnt from 
	( select uid, count(*) as cnt from toc group by uid) a
	join 
	( select uid from tmp_ads_user_2016_all) b
	on a.uid = b.uid
) k 
) h group by h.toc_cnt_bucket 


abort;

