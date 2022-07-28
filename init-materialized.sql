/* 
    |-----------------------------------------------------------|
    |      SERVO SYSTEM INFO FOR THE FRONT RIGHT POSITION       |
    |-----------------------------------------------------------|
*/

CREATE 
SOURCE servo_sys_info_front_right 
FROM KAFKA BROKER 'redpanda:9092' TOPIC 'servo_sys_info_front_right' 
FORMAT BYTES;

CREATE VIEW servo_sys_info_front_right_parsed AS
SELECT *
FROM (
         SELECT (data ->>'id')::text                      AS id,
                (data ->>'iterruptionCount')::int         AS iterruption_count,
                (data ->>'averageAngleSpeed')::float      AS average_angle_speed,
                (data ->>'averageAngleMomentum')::float   AS average_angle_momentum,
                (data ->>'timestamp')::timestamptz        AS timestamp
         FROM (
                  SELECT CAST(data AS jsonb) AS data
                  FROM (
                           SELECT convert_from(data, 'utf8') AS data
                           FROM servo_sys_info_front_right
                       )
              )
     );

CREATE MATERIALIZED VIEW servo_sys_info_front_right_parsed_general 
AS SELECT * FROM servo_sys_info_front_right_parsed;

CREATE MATERIALIZED VIEW servo_sys_info_front_right_parsed_agg 
AS SELECT 
    servo_sys_info_front_right_parsed.id AS id,
    servo_sys_info_front_right_parsed.timestamp::DATE AS recordered_date,
    extract(HOUR FROM servo_sys_info_front_right_parsed.timestamp) AS recordered_hour,
    extract(MINUTE FROM servo_sys_info_front_right_parsed.timestamp) AS recordered_minute,
    AVG (servo_sys_info_front_right_parsed.average_angle_momentum)::NUMERIC(10,2) AS recordered_avg_angle_momentum,
    AVG (servo_sys_info_front_right_parsed.average_angle_speed)::NUMERIC(10,2) AS recordered_avg_angle_speed,
    SUM (servo_sys_info_front_right_parsed.iterruption_count) AS total_iterruption_count
FROM servo_sys_info_front_right_parsed
GROUP BY servo_sys_info_front_right_parsed.timestamp::DATE,
        extract(HOUR FROM servo_sys_info_front_right_parsed.timestamp),
        extract(MINUTE FROM servo_sys_info_front_right_parsed.timestamp),
        servo_sys_info_front_right_parsed.id
ORDER BY servo_sys_info_front_right_parsed.timestamp::DATE DESC,
        extract(HOUR FROM servo_sys_info_front_right_parsed.timestamp) DESC,
        extract(MINUTE FROM servo_sys_info_front_right_parsed.timestamp) DESC;

/* 
    |-----------------------------------------------------------|
    |      SERVO SYSTEM INFO FOR THE FRONT LEFT POSITION        |
    |-----------------------------------------------------------|
*/

CREATE 
SOURCE servo_sys_info_front_left 
FROM KAFKA BROKER 'redpanda:9092' TOPIC 'servo_sys_info_front_left' 
FORMAT BYTES;

CREATE VIEW servo_sys_info_front_left_parsed AS
SELECT *
FROM (
         SELECT (data ->>'id')::text                      AS id,
                (data ->>'iterruptionCount')::int         AS iterruption_count,
                (data ->>'averageAngleSpeed')::float      AS average_angle_speed,
                (data ->>'averageAngleMomentum')::float   AS average_angle_momentum,
                (data ->>'timestamp')::timestamptz        AS timestamp
         FROM (
                  SELECT CAST(data AS jsonb) AS data
                  FROM (
                           SELECT convert_from(data, 'utf8') AS data
                           FROM servo_sys_info_front_left
                       )
              )
     );

CREATE MATERIALIZED VIEW servo_sys_info_front_left_parsed_general 
AS SELECT * FROM servo_sys_info_front_left_parsed;

CREATE MATERIALIZED VIEW servo_sys_info_front_left_parsed_agg 
AS SELECT 
    servo_sys_info_front_left_parsed.id AS id,
    servo_sys_info_front_left_parsed.timestamp::DATE AS recordered_date,
    extract(HOUR FROM servo_sys_info_front_left_parsed.timestamp) AS recordered_hour,
    extract(MINUTE FROM servo_sys_info_front_left_parsed.timestamp) AS recordered_minute,
    AVG (servo_sys_info_front_left_parsed.average_angle_momentum)::NUMERIC(10,2) AS recordered_avg_angle_momentum,
    AVG (servo_sys_info_front_left_parsed.average_angle_speed)::NUMERIC(10,2) AS recordered_avg_angle_speed,
    SUM (servo_sys_info_front_left_parsed.iterruption_count) AS total_iterruption_count
FROM servo_sys_info_front_left_parsed 
GROUP BY servo_sys_info_front_left_parsed.timestamp::DATE,
        extract(HOUR FROM servo_sys_info_front_left_parsed.timestamp),
        extract(MINUTE FROM servo_sys_info_front_left_parsed.timestamp),
        servo_sys_info_front_left_parsed.id
ORDER BY servo_sys_info_front_left_parsed.timestamp::DATE DESC,
        extract(HOUR FROM servo_sys_info_front_left_parsed.timestamp) DESC,
        extract(MINUTE FROM servo_sys_info_front_left_parsed.timestamp) DESC;

/* 
    |-----------------------------------------------------------|
    |      SERVO SYSTEM INFO FOR THE BACK RIGHT POSITION        |
    |-----------------------------------------------------------|
*/

CREATE 
SOURCE servo_sys_info_back_right 
FROM KAFKA BROKER 'redpanda:9092' TOPIC 'servo_sys_info_back_right' 
FORMAT BYTES;

CREATE VIEW servo_sys_info_back_right_parsed AS
SELECT *
FROM (
         SELECT (data ->>'id')::text                      AS id,
                (data ->>'iterruptionCount')::int         AS iterruption_count,
                (data ->>'averageAngleSpeed')::float      AS average_angle_speed,
                (data ->>'averageAngleMomentum')::float   AS average_angle_momentum,
                (data ->>'timestamp')::timestamptz        AS timestamp
         FROM (
                  SELECT CAST(data AS jsonb) AS data
                  FROM (
                           SELECT convert_from(data, 'utf8') AS data
                           FROM servo_sys_info_back_right
                       )
              )
     );

CREATE MATERIALIZED VIEW servo_sys_info_back_right_parsed_general 
AS SELECT * FROM servo_sys_info_back_right_parsed;

CREATE MATERIALIZED VIEW servo_sys_info_back_right_parsed_agg
AS SELECT 
    servo_sys_info_back_right_parsed.id AS id,
    servo_sys_info_back_right_parsed.timestamp::DATE AS recordered_date,
    extract(HOUR FROM servo_sys_info_back_right_parsed.timestamp) AS recordered_hour,
    extract(MINUTE FROM servo_sys_info_back_right_parsed.timestamp) AS recordered_minute,
    AVG (servo_sys_info_back_right_parsed.average_angle_momentum)::NUMERIC(10,2) AS recordered_avg_angle_momentum,
    AVG (servo_sys_info_back_right_parsed.average_angle_speed)::NUMERIC(10,2) AS recordered_avg_angle_speed,
    SUM (servo_sys_info_back_right_parsed.iterruption_count) AS total_iterruption_count
FROM servo_sys_info_back_right_parsed 
GROUP BY servo_sys_info_back_right_parsed.timestamp::DATE,
        extract(HOUR FROM servo_sys_info_back_right_parsed.timestamp),
        extract(MINUTE FROM servo_sys_info_back_right_parsed.timestamp),
        servo_sys_info_back_right_parsed.id
ORDER BY servo_sys_info_back_right_parsed.timestamp::DATE DESC,
        extract(HOUR FROM servo_sys_info_back_right_parsed.timestamp) DESC,
        extract(MINUTE FROM servo_sys_info_back_right_parsed.timestamp) DESC;

/* 
    |-----------------------------------------------------------|
    |      SERVO SYSTEM INFO FOR THE BACK LEFT POSITION         |
    |-----------------------------------------------------------|
*/

CREATE 
SOURCE servo_sys_info_back_left 
FROM KAFKA BROKER 'redpanda:9092' TOPIC 'servo_sys_info_back_left' 
FORMAT BYTES;

CREATE VIEW servo_sys_info_back_left_parsed AS
SELECT *
FROM (
         SELECT (data ->>'id')::text                      AS id,
                (data ->>'iterruptionCount')::int         AS iterruption_count,
                (data ->>'averageAngleSpeed')::float      AS average_angle_speed,
                (data ->>'averageAngleMomentum')::float   AS average_angle_momentum,
                (data ->>'timestamp')::timestamptz        AS timestamp
         FROM (
                  SELECT CAST(data AS jsonb) AS data
                  FROM (
                           SELECT convert_from(data, 'utf8') AS data
                           FROM servo_sys_info_back_left
                       )
              )
     );

CREATE MATERIALIZED VIEW servo_sys_info_back_left_parsed_general 
AS SELECT * FROM servo_sys_info_back_left_parsed;

CREATE MATERIALIZED VIEW servo_sys_info_back_left_parsed_agg
AS SELECT 
    servo_sys_info_back_left_parsed.id AS id,
    servo_sys_info_back_left_parsed.timestamp::DATE AS recordered_date,
    extract(HOUR FROM servo_sys_info_back_left_parsed.timestamp) AS recordered_hour,
    extract(MINUTE FROM servo_sys_info_back_left_parsed.timestamp) AS recordered_minute,
    AVG (servo_sys_info_back_left_parsed.average_angle_momentum)::NUMERIC(10,2) AS recordered_avg_angle_momentum,
    AVG (servo_sys_info_back_left_parsed.average_angle_speed)::NUMERIC(10,2) AS recordered_avg_angle_speed,
    SUM (servo_sys_info_back_left_parsed.iterruption_count) AS total_iterruption_count
FROM servo_sys_info_back_left_parsed 
GROUP BY servo_sys_info_back_left_parsed.timestamp::DATE,
        extract(HOUR FROM servo_sys_info_back_left_parsed.timestamp),
        extract(MINUTE FROM servo_sys_info_back_left_parsed.timestamp),
        servo_sys_info_back_left_parsed.id
ORDER BY servo_sys_info_back_left_parsed.timestamp::DATE DESC,
        extract(HOUR FROM servo_sys_info_back_left_parsed.timestamp) DESC,
        extract(MINUTE FROM servo_sys_info_back_left_parsed.timestamp) DESC;

/* 
    |-----------------------------------------------------------|
    |      SERVO SYSTEM INFO AGGREGATION CONTROL TABLE          |
    |-----------------------------------------------------------|
*/
CREATE MATERIALIZED VIEW servo_sys_info_agg
AS SELECT
    bl.recordered_date,
    bl.recordered_hour,
    bl.recordered_minute,
    ABS(bl.recordered_avg_angle_momentum - br.recordered_avg_angle_momentum) AS bl_br_avg_angle_momentum_diff,
    ABS(bl.recordered_avg_angle_momentum - fr.recordered_avg_angle_momentum) AS bl_fr_avg_angle_momentum_diff,
    ABS(bl.recordered_avg_angle_momentum - fl.recordered_avg_angle_momentum) AS bl_fl_avg_angle_momentum_diff,
    ABS(br.recordered_avg_angle_momentum - fl.recordered_avg_angle_momentum) AS br_fl_avg_angle_momentum_diff,
    ABS(br.recordered_avg_angle_momentum - fr.recordered_avg_angle_momentum) AS br_fr_avg_angle_momentum_diff,
    ABS(fr.recordered_avg_angle_momentum - fl.recordered_avg_angle_momentum) AS fr_fl_avg_angle_momentum_diff,
    ABS(bl.recordered_avg_angle_speed - br.recordered_avg_angle_speed) AS bl_br_avg_angle_speed_diff,
    ABS(bl.recordered_avg_angle_speed - fr.recordered_avg_angle_speed) AS bl_fr_avg_angle_speed_diff,
    ABS(bl.recordered_avg_angle_speed - fl.recordered_avg_angle_speed) AS bl_fl_avg_angle_speed_diff,
    ABS(br.recordered_avg_angle_speed - fl.recordered_avg_angle_speed) AS br_fl_avg_angle_speed_diff,
    ABS(br.recordered_avg_angle_speed - fr.recordered_avg_angle_speed) AS br_fr_avg_angle_speed_diff,
    ABS(fr.recordered_avg_angle_speed - fl.recordered_avg_angle_speed) AS fr_fl_avg_angle_speed_diff,
    bl.total_iterruption_count + 
    br.total_iterruption_count + 
    fl.total_iterruption_count +
    fr.total_iterruption_count AS total_iterruption_count,
    bl.id AS bl_id,
    br.id AS br_id,
    fl.id AS fl_id,
    fr.id AS fr_id
FROM servo_sys_info_back_left_parsed_agg as bl
INNER JOIN servo_sys_info_back_right_parsed_agg as br
    ON bl.recordered_date = br.recordered_date
    AND bl.recordered_hour = br.recordered_hour
    AND bl.recordered_minute = br.recordered_minute
INNER JOIN servo_sys_info_front_left_parsed_agg as fl
    ON bl.recordered_date = fl.recordered_date
    AND bl.recordered_hour = fl.recordered_hour
    AND bl.recordered_minute = fl.recordered_minute
INNER JOIN servo_sys_info_front_right_parsed_agg as fr
    ON bl.recordered_date = fr.recordered_date
    AND bl.recordered_hour = fr.recordered_hour
    AND bl.recordered_minute = fr.recordered_minute;

/* 
    |-----------------------------------------------------------|
    |  STREAMING THE AVERAGE INFORMATION BACK TO THE REDPANDA   |
    |-----------------------------------------------------------|
*/
-- CREATE SINK servo_sys_info_agg_sink123
--     FROM servo_sys_info_agg
--     INTO KAFKA BROKER 'redpanda:9092' TOPIC 'servo_sys_info_agg'
--     WITH (reuse_topic=true, consistency_topic='servo_sys_info_agg-consistency')
--     FORMAT AVRO USING
--     CONFLUENT SCHEMA REGISTRY 'http://redpanda:8081';

CREATE SINK servo_sys_info_agg_sink
    FROM servo_sys_info_agg
    INTO KAFKA BROKER 'redpanda:9092' TOPIC 'servo_sys_info_agg'
    FORMAT JSON;