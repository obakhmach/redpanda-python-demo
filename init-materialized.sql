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
                (data ->>'timestamp')::float          AS timestamp
         FROM (
                  SELECT CAST(data AS jsonb) AS data
                  FROM (
                           SELECT convert_from(data, 'utf8') AS data
                           FROM servo_sys_info_front_right
                       )
              )
     );

CREATE MATERIALIZED VIEW servo_sys_info_front_right_parsed_general AS SELECT * FROM servo_sys_info_front_right_parsed;