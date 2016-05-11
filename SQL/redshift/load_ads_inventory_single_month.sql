unload ('select uid, sum(inventory), prod_type, appmode, countryname from ads where dayhour > \'2016-04-11\' and dayhour < \'2016-05-11\' and uid != 0 and event_action = \'inventory\' group by uid, prod_type, appmode, countryname') to 's3://../tmp/gfan/ads/2016/part'
CREDENTIALS 'aws_access_key_id=...' delimiter '\t';

CREATE TABLE tmp_ads_inventory_2016_04 (
    uid int encode lzo,
    inventory int encode lzo,
    prod_type varchar encode lzo,
    appmode varchar encode lzo,
    countryname varchar encode lzo
);

COPY core_schema.tmp_ads_inventory_2016_04 FROM 's3://../tmp/gfan/ads/2016' CREDENTIALS 'aws_access_key_id=...' delimiter '\t' emptyasnull blanksasnull maxerror 2000 ignoreblanklines TRIMBLANKS TRUNCATECOLUMNS;
