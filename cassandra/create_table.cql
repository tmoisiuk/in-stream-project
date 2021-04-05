DROP KEYSPACE IF EXISTS BotDetection;
CREATE KEYSPACE BotDetection
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
USE BotDetection;

DROP TABLE IF EXISTS EnrichedClicks;
CREATE TABLE EnrichedClicks (

type TEXT,
ip TEXT PRIMARY KEY,
is_bot BOOLEAN,
event_time BIGINT,
url TEXT
);