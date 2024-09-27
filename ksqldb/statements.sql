CREATE STREAM position_stream (
    coursierId STRING KEY,
    position STRUCT<latitude DOUBLE, longitude DOUBLE>
) WITH (
    KAFKA_TOPIC = 'position',
    VALUE_FORMAT = 'JSON'
);

CREATE STREAM statut_stream (
    coursierId STRING KEY,
    statut STRING,
    firstName STRING,
    lastName STRING
) WITH (
    KAFKA_TOPIC = 'statutCoursier',
    VALUE_FORMAT = 'JSON'
);

CREATE STREAM position_statut AS
    SELECT c.coursierId,
           c.firstName,
           c.lastName,
           c.statut,
	   p.position->latitude AS latitude,
           p.position->longitude AS longitude
    FROM statut_stream c
    JOIN position_stream p
    WITHIN 5 MINUTES -- Fenêtre de jointure de 5 minutes
    GRACE PERIOD 1 MINUTE 
    ON c.coursierId = p.coursierId
    EMIT CHANGES;
CREATE STREAM position_statut AS
    SELECT c.coursierId,
           c.firstName,
           c.lastName,
           c.statut,
	   p.position
    FROM statut_stream c
    JOIN position_stream p
    WITHIN 5 MINUTES -- Fenêtre de jointure de 5 minutes
    GRACE PERIOD 1 MINUTE 
    ON c.coursierId = p.coursierId
    EMIT CHANGES;
select FROM_UNIXTIME(ROWTIME),* from position_statut EMIT CHANGES;

----------------------------------------
CREATE TABLE coursier_table (
    coursierId STRING PRIMARY KEY,
    firstName STRING,
    lastName STRING,
    statut STRING,
    position STRUCT<latitude DOUBLE, longitude DOUBLE>
) WITH (
    KAFKA_TOPIC = 'POSITION_STATUT',
    VALUE_FORMAT = 'JSON'
);

CREATE TABLE Coursier_libre AS
  select * from coursier_table where statut='LIBRE';

select * from Coursier_libre;

select * from Coursier_libre emit changes;

--------------------------------------------
SELECT count(*) from Coursier_libre;

SELECT COUNT_DISTINCT(coursierId)
FROM statut_stream
  WINDOW HOPPING (SIZE 30 SECONDS, ADVANCE BY 10 SECONDS)
WHERE statut = 'LIBRE'
EMIT CHANGES;

SELECT COUNT_DISTINCT(coursierId) AS distinct_coursiers
FROM statut_stream
WINDOW TUMBLING (SIZE 30 SECONDS)
WHERE statut = 'LIBRE'
EMIT CHANGES;

SELECT AVG(position->latitude), AVG(position->longitude) from position_stream EMIT CHANGES;

--- 
SELECT FROM_UNIXTIME(ROWTIME),coursierId, ROUND(GEO_DISTANCE(position->latitude, position->longitude, 37.4133, -122.1162), -1) AS distanceInMiles from position_stream;

SELECT FROM_UNIXTIME(ROWTIME),coursierId, ROUND(GEO_DISTANCE(position->latitude, position->longitude, 37.4133, -122.1162), -1) AS distanceInMiles from position_stream EMIT CHANGES;
