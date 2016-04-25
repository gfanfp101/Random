unload ('select sum(inventory), uid, prod_type from ads where dayhour > \'2016-01-01\' and uid != 0 and event_action = \'inventory\' and countryname = \'United States\' group by uid, prod_type') to 's3://.../tmp/gfan/ads/2016/part'
CREDENTIALS 'aws_access_key_id=...' delimiter '\t';


unload ('with inv as (select sum(inventory) as s, uid, prod_type, substring(dayhour, 0, 8) as m from ads where dayhour > \'2016-01-01\' and uid != 0 and event_action = \'inventory\' and countryname = \'United States\' group by uid, prod_type, substring(dayhour, 0, 8)) 
select sum(s), max(prod_type), uid from inv where s >= 44 group by uid') to 's3://.../tmp/gfan/ads/2016/part'
CREDENTIALS 'aws_access_key_id=...' delimiter '\t';


unload ('select distinct uid, prod_type from ads where dayhour > \'2016-01-01\' and uid != 0 and event_action = \'inventory\' and countryname = \'United States\' ) 
select sum(s), max(prod_type), uid from inv where s >= 44 group by uid') to 's3://.../tmp/gfan/ads/2016/part'
CREDENTIALS 'aws_access_key_id=...' delimiter '\t';


CREATE TABLE tmp_ads_inventory_2016 (
    inventory int encode lzo,
    prod_type varchar encode lzo,
    uid int encode lzo
);

COPY core_schema.tmp_ads_inventory_2016 FROM 's3://.../tmp/gfan/ads/2016' CREDENTIALS 'aws_access_key_id=...' delimiter ' \t' gzip emptyasnull blanksasnull maxerror 2000 ignoreblanklines TRIMBLANKS TRUNCATECOLUMNS;


with toc_inv as (
select t.uid, a.prod_type, t.title, t.type
from toc t
join
tmp_ads_inventory_2016 a
on t.uid = a.uid
)
select toc_inv.title as title, max(type) as type, toc_inv.prod_type as prod_type, count(distinct uid) as cnt
into tmp_toc_mvu_2016
from toc_inv
group by toc_inv.title, toc_inv.prod_type
;

with toc_inv as (
select t.uid, a.prod_type, t.title, t.type
from toc t
join
tmp_all_user_2016 a
on t.uid = a.uid
)
select toc_inv.title as title, max(type) as type, toc_inv.prod_type as prod_type, count(distinct uid) as cnt
into tmp_toc_all_2016
from toc_inv
group by toc_inv.title, toc_inv.prod_type
;


drop table tmp_toc_mvu_2016;

with cnt_t as (select title, max(type) as type, sum(cnt) as cnt from tmp_toc_all_2016 group by title)
select title, type, cnt
from cnt_t
order by cnt desc
limit 500;

select prod_type, count(*) from tmp_toc_mvu_2016 group by prod_type;

select * from tmp_toc_all_2016 where prod_type = 'ios' order by cnt desc limit 500;
