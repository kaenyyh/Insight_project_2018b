DROP KEYSPACE IF EXISTS ks;

CREATE KEYSPACE ks
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };


CREATE TABLE IF NOT EXISTS ks.totalInputCountSecond (
          global_id       text,
          proc_time       text,
          count           bigint,
          PRIMARY KEY (global_id, proc_time)
	  ) with clustering order by (proc_time DESC);

CREATE TABLE IF NOT EXISTS ks.singleUserCount(
          username        text,
          proc_time       text,
          count           bigint,
          PRIMARY KEY     (username, proc_time)
          ) with clustering order by (proc_time DESC);

CREATE TABLE IF NOT EXISTS ks.flaggedUser(
          username        text,
          proc_time       text,
          count           bigint,
          PRIMARY KEY     (username, proc_time)
          ) with clustering order by (proc_time DESC);

