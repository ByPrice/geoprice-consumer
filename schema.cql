/* DROP KEYSPACE IF EXISTS geoprice;*/

/* Local & Dev setup */
/* CREATE KEYSPACE geoprice WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = true; USE geoprice_test; */

/* Remote setup */
/* CREATE KEYSPACE geoprice WITH replication = {'class': 'NetworkTopologyStrategy', 'Analytics': '1', 'Cassandra': '1'}  AND durable_writes = true; */

/* command start */
/* Prices by product_uuid ordered by time desc */
CREATE TABLE price (
    product_uuid    uuid,
    time            timestamp,
    gtin            int,     
    store_uuid      uuid,
    lat             float,
    lng             float,
    price           float,
    price_original  float,
    promo           text,
    url             text,
    currency        text,
    PRIMARY KEY     (product_uuid, time)    
) WITH CLUSTERING ORDER BY (time DESC);
/* command end */

/* command start */
/* Prices by product and date */
CREATE TABLE price_by_product_date (
    product_uuid    uuid,
    date            int,
    time            timestamp,
    store_uuid      uuid,
    price           float,
    price_original  float,
    promo           text,
    currency        text,
    PRIMARY KEY     ((product_uuid, date), time)    
) WITH CLUSTERING ORDER BY (time DESC);
/* command end */

/* command start */
/* Prices by date */
CREATE TABLE price_by_date (
    date            int,
    time            timestamp,
    product_uuid    uuid,
    store_uuid      uuid,
    price           float,
    price_original  float,
    promo           text,
    url             text,
    currency        text,
    PRIMARY KEY     (date, time, product_uuid)   
) WITH CLUSTERING ORDER BY (time DESC, product_uuid ASC);
/* command end */

/* command start */
/* Prices by product and store */
CREATE TABLE price_by_product_store (
    product_uuid    uuid,
    store_uuid      uuid,
    time            timestamp,
    lat             float,
    lng             float,
    price           float,
    price_original  float,
    promo           text,
    url             text,
    currency        text,
    PRIMARY KEY     ((product_uuid, store_uuid), time)
) WITH CLUSTERING ORDER BY (time DESC);
/* command end */

/* command start */
/* Prices by geohash */
CREATE TABLE price_by_geohash (
    product_uuid    uuid,
    geohash         text,
    time            timestamp,
    source          text,
    store_uuid      uuid,
    lat             float,
    lng             float,
    price           float,
    price_original  float,
    promo           text,
    url             text,
    currency        text,
    PRIMARY KEY     ((product_uuid, geohash), time)   
) WITH CLUSTERING ORDER BY (time DESC);
/* command end */

/* command start */
/* Prices by source */
CREATE TABLE price_by_source (
    source          text,
    date            int,
    time            timestamp,
    product_uuid    uuid,
    store_uuid      uuid,
    lat             float,
    lng             float,
    price           float,
    price_original  float,
    promo           text,
    url             text,
    currency        text,
    PRIMARY KEY     ((source, date), time, product_uuid)
) WITH CLUSTERING ORDER BY (time DESC, product_uuid ASC);
/* command end */

/* command start */
/* Prices by store */
CREATE TABLE price_by_store (
    store_uuid      uuid,
    date            int, 
    time            timestamp,
    product_uuid    uuid,
    lat             float,
    lng             float,
    price           float,
    price_original  float,
    promo           text,
    url             text,
    currency        text,
    PRIMARY KEY     ((store_uuid, date), time, product_uuid)
) WITH CLUSTERING ORDER BY (time DESC, product_uuid ASC);
/* command end */

/* command start */
/* Promo by product */
CREATE TABLE promo (
    product_uuid    uuid,
    time            timestamp,
    store_uuid      uuid,
    lat             float,
    lng             float,
    price           float,
    price_original  float,
    promo           text,
    url             text,
    currency        text,
    PRIMARY KEY     (product_uuid, time, store_uuid)
) WITH CLUSTERING ORDER BY (time DESC, store_uuid ASC);
/* command end */

/* command start */
/* Promos */
CREATE TABLE promo_by_store (
    product_uuid    uuid,
    time            timestamp,
    store_uuid      uuid,
    lat             float,
    lng             float,
    price           float,
    price_original  float,
    promo           text,
    url             text,
    currency        text,
    PRIMARY KEY     (store_uuid, time, product_uuid)
) WITH CLUSTERING ORDER BY (time DESC, product_uuid ASC);
/* command end */

/* command start */
/* Raw prices */
CREATE TABLE price_raw (
    date            int,
    product_uuid    uuid,
    raw             text,
    PRIMARY KEY     (date, product_uuid)    
) WITH CLUSTERING ORDER BY (product_uuid ASC);
/* command end */

/* command start */
/* Stats */
CREATE TABLE stats_by_product (
    product_uuid    uuid,
    date            int, 
    avg_price       float,
    datapoints      int,
    max_price       float,
    min_price       float,
    mode_price      float,
    std_price       float,
    PRIMARY KEY     (product_uuid, date)
) WITH CLUSTERING ORDER BY (date DESC);
/* command end */
