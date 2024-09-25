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
    ON c.coursierId = p.coursierId
    EMIT CHANGES;

select * from position_statut EMIT CHANGES;

----------------------------------------
CREATE TABLE position_table (
    coursierId STRING PRIMARY KEY,
    position STRUCT<latitude DOUBLE, longitude DOUBLE>
) WITH (
    KAFKA_TOPIC = 'position',
    VALUE_FORMAT = 'JSON'
);

select * from position_table emit changes;

--------------------------------------------
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

--- 
SELECT coursierId, ROUND(GEO_DISTANCE(position->latitude, position->longitude, 37.4133, -122.1162), -1) AS distanceInMiles from position_stream;

SELECT coursierId, ROUND(GEO_DISTANCE(position->latitude, position->longitude, 37.4133, -122.1162), -1) AS distanceInMiles from position_stream EMIT CHANGES;
