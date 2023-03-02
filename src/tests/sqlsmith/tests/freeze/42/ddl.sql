CREATE TABLE supplier (s_suppkey INT, s_name CHARACTER VARYING, s_address CHARACTER VARYING, s_nationkey INT, s_phone CHARACTER VARYING, s_acctbal NUMERIC, s_comment CHARACTER VARYING, PRIMARY KEY (s_suppkey));
CREATE TABLE part (p_partkey INT, p_name CHARACTER VARYING, p_mfgr CHARACTER VARYING, p_brand CHARACTER VARYING, p_type CHARACTER VARYING, p_size INT, p_container CHARACTER VARYING, p_retailprice NUMERIC, p_comment CHARACTER VARYING, PRIMARY KEY (p_partkey));
CREATE TABLE partsupp (ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost NUMERIC, ps_comment CHARACTER VARYING, PRIMARY KEY (ps_partkey, ps_suppkey));
CREATE TABLE customer (c_custkey INT, c_name CHARACTER VARYING, c_address CHARACTER VARYING, c_nationkey INT, c_phone CHARACTER VARYING, c_acctbal NUMERIC, c_mktsegment CHARACTER VARYING, c_comment CHARACTER VARYING, PRIMARY KEY (c_custkey));
CREATE TABLE orders (o_orderkey BIGINT, o_custkey INT, o_orderstatus CHARACTER VARYING, o_totalprice NUMERIC, o_orderdate DATE, o_orderpriority CHARACTER VARYING, o_clerk CHARACTER VARYING, o_shippriority INT, o_comment CHARACTER VARYING, PRIMARY KEY (o_orderkey));
CREATE TABLE lineitem (l_orderkey BIGINT, l_partkey INT, l_suppkey INT, l_linenumber INT, l_quantity NUMERIC, l_extendedprice NUMERIC, l_discount NUMERIC, l_tax NUMERIC, l_returnflag CHARACTER VARYING, l_linestatus CHARACTER VARYING, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct CHARACTER VARYING, l_shipmode CHARACTER VARYING, l_comment CHARACTER VARYING, PRIMARY KEY (l_orderkey, l_linenumber));
CREATE TABLE nation (n_nationkey INT, n_name CHARACTER VARYING, n_regionkey INT, n_comment CHARACTER VARYING, PRIMARY KEY (n_nationkey));
CREATE TABLE region (r_regionkey INT, r_name CHARACTER VARYING, r_comment CHARACTER VARYING, PRIMARY KEY (r_regionkey));
CREATE TABLE person (id BIGINT, name CHARACTER VARYING, email_address CHARACTER VARYING, credit_card CHARACTER VARYING, city CHARACTER VARYING, state CHARACTER VARYING, date_time TIMESTAMP, extra CHARACTER VARYING, PRIMARY KEY (id));
CREATE TABLE auction (id BIGINT, item_name CHARACTER VARYING, description CHARACTER VARYING, initial_bid BIGINT, reserve BIGINT, date_time TIMESTAMP, expires TIMESTAMP, seller BIGINT, category BIGINT, extra CHARACTER VARYING, PRIMARY KEY (id));
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel CHARACTER VARYING, url CHARACTER VARYING, date_time TIMESTAMP, extra CHARACTER VARYING);
CREATE TABLE alltypes1 (c1 BOOLEAN, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 NUMERIC, c8 DATE, c9 CHARACTER VARYING, c10 TIME, c11 TIMESTAMP, c13 INTERVAL, c14 STRUCT<a INT>, c15 INT[], c16 CHARACTER VARYING[]);
CREATE TABLE alltypes2 (c1 BOOLEAN, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 NUMERIC, c8 DATE, c9 CHARACTER VARYING, c10 TIME, c11 TIMESTAMP, c13 INTERVAL, c14 STRUCT<a INT>, c15 INT[], c16 CHARACTER VARYING[]);
CREATE MATERIALIZED VIEW m0 AS WITH with_0 AS (SELECT t_2.n_regionkey AS col_0, (t_2.n_regionkey * (SMALLINT '408')) AS col_1, t_1.s_comment AS col_2 FROM supplier AS t_1 LEFT JOIN nation AS t_2 ON t_1.s_nationkey = t_2.n_regionkey AND ((INTERVAL '-60') IS NULL) GROUP BY t_2.n_regionkey, t_2.n_nationkey, t_1.s_comment, t_1.s_name, t_2.n_comment HAVING true) SELECT (-1854102740) AS col_0, ((SMALLINT '32767') % (SMALLINT '-32768')) AS col_1 FROM with_0;
CREATE MATERIALIZED VIEW m1 AS SELECT 'izwv13YfIe' AS col_0, sq_1.col_1 AS col_1, sq_1.col_1 AS col_2 FROM (SELECT (concat_ws(tumble_0.item_name, 'cUmMVnR8kP', tumble_0.item_name, ('PbnlAAoozQ'))) AS col_0, tumble_0.item_name AS col_1 FROM tumble(auction, auction.expires, INTERVAL '87') AS tumble_0 WHERE true GROUP BY tumble_0.initial_bid, tumble_0.item_name, tumble_0.reserve, tumble_0.id, tumble_0.date_time HAVING true) AS sq_1 GROUP BY sq_1.col_1;
CREATE MATERIALIZED VIEW m2 AS SELECT t_0.col_2 AS col_0, t_0.col_2 AS col_1, t_0.col_2 AS col_2, (ARRAY['JwqRG0lBy3']) AS col_3 FROM m1 AS t_0 LEFT JOIN lineitem AS t_1 ON t_0.col_1 = t_1.l_comment WHERE true GROUP BY t_0.col_2 HAVING false;
CREATE MATERIALIZED VIEW m4 AS WITH with_0 AS (SELECT sq_2.col_1 AS col_0, sq_2.col_0 AS col_1, sq_2.col_0 AS col_2, sq_2.col_1 AS col_3 FROM (SELECT (INT '463') AS col_0, 'hTLFwtJOS9' AS col_1, 'YtxZiYg4yT' AS col_2, t_1.col_0 AS col_3 FROM m1 AS t_1 GROUP BY t_1.col_0) AS sq_2 WHERE true GROUP BY sq_2.col_1, sq_2.col_0 HAVING true) SELECT (212) AS col_0 FROM with_0 WHERE false;
CREATE MATERIALIZED VIEW m5 AS SELECT t_1.o_custkey AS col_0, 'ZzFeh040go' AS col_1, t_0.p_name AS col_2 FROM part AS t_0 RIGHT JOIN orders AS t_1 ON t_0.p_mfgr = t_1.o_clerk WHERE (t_0.p_size > ((SMALLINT '256') # t_1.o_custkey)) GROUP BY t_0.p_brand, t_0.p_type, t_0.p_name, t_1.o_custkey HAVING false;
CREATE MATERIALIZED VIEW m6 AS SELECT t_0.name AS col_0, t_0.name AS col_1, t_0.state AS col_2, 'QIH48tnopO' AS col_3 FROM person AS t_0 RIGHT JOIN partsupp AS t_1 ON t_0.city = t_1.ps_comment AND ((FLOAT '1049254225') = t_1.ps_suppkey) GROUP BY t_1.ps_comment, t_0.name, t_0.state;
CREATE MATERIALIZED VIEW m8 AS SELECT hop_0.state AS col_0, hop_0.name AS col_1 FROM hop(person, person.date_time, INTERVAL '60', INTERVAL '4440') AS hop_0 GROUP BY hop_0.state, hop_0.name;
CREATE MATERIALIZED VIEW m9 AS SELECT (OVERLAY(t_2.s_phone PLACING (md5(t_2.s_phone)) FROM t_2.s_suppkey)) AS col_0, t_2.s_suppkey AS col_1, ((SMALLINT '751') % t_2.s_suppkey) AS col_2 FROM supplier AS t_2 WHERE (CASE WHEN ((((REAL '679')) * (FLOAT '328')) <= t_2.s_acctbal) THEN false WHEN (t_2.s_nationkey = (BIGINT '101')) THEN false WHEN false THEN ((FLOAT '69') <= t_2.s_suppkey) ELSE (t_2.s_nationkey < t_2.s_suppkey) END) GROUP BY t_2.s_phone, t_2.s_suppkey, t_2.s_comment;
