DROP KEYSPACE IF EXISTS ks;

CREATE KEYSPACE ks
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };


CREATE TABLE IF NOT EXISTS ks.totalInputCountSecond (
          global_id       text,
          proc_time       text,
          count           bigint,
          PRIMARY KEY (global_id, time)
	  ) with clustering order by (time DESC);

CREATE TABLE IF NOT EXISTS ks.singleUserActivity(
          username        text,
          proc_time       text,
          count           bigint,
          PRIMARY KEY     (username, time)
          ) with clustering order by (time DESC);

