/* Create a Time Series Schema */
CREATE SCHEMA time_series
    AUTHORIZATION postgres;




/* Create a table of location and temperature measurements */
CREATE TABLE time_series.location_temp
(
    event_time timestamp without time zone NOT NULL,
    temp_celcius integer,
    location_id character varying COLLATE pg_catalog."default"
);


ALTER TABLE time_series.location_temp OWNER to postgres;






/* Create table of server monitoring metrics */
CREATE TABLE time_series.utilization
(
    event_time timestamp without time zone NOT NULL,
    server_id integer NOT NULL,
    cpu_utilization real,
    free_memory real,
    session_cnt integer,
    CONSTRAINT utilization_pkey PRIMARY KEY (event_time, server_id)
);


ALTER TABLE time_series.utilization  OWNER to postgres;




/*Loading Data*/
COPY time_series.location_temp(event_time, location_id,temp_celcius)
FROM 'C:/Users/v-trugan/OneDrive - Microsoft/Desktop/TruptiFolder2020/Python_exercise/postgres/Time_Series/data/location_temp.txt'
DELIMITER ',';

COPY time_series.utilization(event_time, server_id, cpu_utilization, free_memory, session_cnt)
FROM 'C:/Users/v-trugan/OneDrive - Microsoft/Desktop/TruptiFolder2020/Python_exercise/postgres/Time_Series/data/utilization.txt'
DELIMITER ',';



-- check the table
SELECT * FROM time_series.location_temp
LIMIT 10;

SELECT * FROM time_series.utilization
LIMIT 10;

--- Analyze

SELECT * FROM time_series.location_temp
order by location_id
LIMIT 10;


SELECT 
location_id, AVG(temp_celcius)
FROM time_series.location_temp
Group by location_id;

EXPLAIN ANALYZE SELECT 
location_id, AVG(temp_celcius)
FROM time_series.location_temp
Group by location_id; 
-- EXECUTION TIME - 80.276

-- Created index
CREATE INDEX idx_loc_temp_locID 
ON time_series.location_temp(location_id);

EXPLAIN ANALYZE SELECT 
location_id, AVG(temp_celcius)
FROM time_series.location_temp
where location_id = 'loc2'
Group by location_id;
-- Query used BITMAP INDEX and completed in .412 ms

Drop INDEX time_series.idx_loc_temp_locID;

SELECT 
	location_id, AVG(temp_celcius)
FROM
	time_series.location_temp
WHERE
	event_time BETWEEN '2019-03-05' AND '2019-03-06'
Group by 
	location_id; 
	
Explain SELECT 
	location_id, AVG(temp_celcius)
FROM
	time_series.location_temp
WHERE
	event_time BETWEEN '2019-03-05' AND '2019-03-06'
Group by 
	location_id; 	
--   ->  Gather Merge  (cost=7636.11..7752.78 rows=1000 width=38)

--Create index
CREATE INDEX idx_loc_temp_time_loc ON 
time_series.location_temp(event_time, location_id);

-- After indexing
Explain SELECT 
	location_id, AVG(temp_celcius)
FROM
	time_series.location_temp
WHERE
	event_time BETWEEN '2019-03-05' AND '2019-03-06'
Group by 
	location_id; 
	
-- time_series.utilization
SELECT * FROM time_series.utilization
LIMIT 10;

SELECT *, server_id % 10 as dept_id FROM time_series.utilization
LIMIT 10;

-- create view
CREATE VIEW time_series.v_utilization as
(SELECT *, server_id % 10 as dept_id FROM time_series.utilization)

SELECT 
	dept_id, server_id, cpu_utilization,
	LEAD(cpu_utilization,2 ) OVER (PARTITION BY dept_id ORDER BY cpu_utilization DESC )
FROM 	
	time_series.v_utilization
WHERE
	event_time BETWEEN '2019-03-05' AND '2019-03-06';


SELECT 
	dept_id, server_id, cpu_utilization,
	LAG(cpu_utilization,2 ) OVER (PARTITION BY dept_id ORDER BY cpu_utilization DESC )
FROM 	
	time_series.v_utilization
WHERE
	event_time BETWEEN '2019-03-05' AND '2019-03-06';
	
	
---	RANK FUNCTION - count rank and increment rank for partition
	SELECT 
	dept_id, server_id, cpu_utilization,
	RANK() OVER (PARTITION BY dept_id ORDER BY cpu_utilization DESC )
FROM 	
	time_series.v_utilization
WHERE
	event_time BETWEEN '2019-03-05' AND '2019-03-06';
	

---- Avg of cpu_utilization per server
SELECT
  server_id, cpu_utilization,
  avg(cpu_utilization) OVER (PARTITION BY server_id)
FROM
  time_series.utilization
WHERE
  event_time BETWEEN '2019-03-05' AND '2019-03-06';
  
  
 SELECT
  regr_slope(free_memory, cpu_utilization) m,
  regr_intercept(free_memory, cpu_utilization) b
FROM
  time_series.utilization
WHERE
  event_time BETWEEN '2019-03-05' AND '2019-03-06'; 
  
/* y = mx + b
   m = slope
   b = intercept
   x = input value
   y = predicted value
*/

SELECT
  regr_slope(free_memory, cpu_utilization) * 0.60 +
  regr_intercept(free_memory, cpu_utilization) predicted_free_memory
FROM
  time_series.utilization
WHERE
  event_time BETWEEN '2019-03-05' AND '2019-03-06';  
  

/* Create a table of location and temperature measurements */
CREATE TABLE time_series.location_temp_p
(
    event_time timestamp NOT NULL,
    event_hour integer,
    temp_celcius integer,
    location_id character varying COLLATE pg_catalog."default"
)
 PARTITION BY RANGE (event_hour);


CREATE TABLE time_series.loc_temp_p1 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (0) TO (2);
CREATE INDEX idx_loc_temp_p1 ON time_series.loc_temp_p1(event_time);


CREATE TABLE time_series.loc_temp_p2 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (2) TO (4);
CREATE INDEX idx_loc_temp_p2 ON time_series.loc_temp_p2(event_time);


CREATE TABLE time_series.loc_temp_p3 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (4) TO (6);
CREATE INDEX idx_loc_temp_p3 ON time_series.loc_temp_p3(event_time);


CREATE TABLE time_series.loc_temp_p4 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (6) TO (8);
CREATE INDEX idx_loc_temp_p4 ON time_series.loc_temp_p4(event_time);


CREATE TABLE time_series.loc_temp_p5 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (8) TO (10);
CREATE INDEX idx_loc_temp_p5 ON time_series.loc_temp_p5(event_time);


CREATE TABLE time_series.loc_temp_p6 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (10) TO (12);
CREATE INDEX idx_loc_temp_p6 ON time_series.loc_temp_p6(event_time);


CREATE TABLE time_series.loc_temp_p7 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (12) TO (14);
CREATE INDEX idx_loc_temp_p7 ON time_series.loc_temp_p7(event_time);


CREATE TABLE time_series.loc_temp_p8 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (14) TO (16);
CREATE INDEX idx_loc_temp_p8 ON time_series.loc_temp_p8(event_time);


CREATE TABLE time_series.loc_temp_p9 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (16) TO (18);
CREATE INDEX idx_loc_temp_9 ON time_series.loc_temp_p9(event_time);


CREATE TABLE time_series.loc_temp_p10 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (18) TO (20);
CREATE INDEX idx_loc_temp_p10 ON time_series.loc_temp_p10(event_time);


CREATE TABLE time_series.loc_temp_p11 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (20) TO (22);
CREATE INDEX idx_loc_temp_p11 ON time_series.loc_temp_p11(event_time);


CREATE TABLE time_series.loc_temp_p12 PARTITION OF time_series.location_temp_p
    FOR VALUES FROM (22) TO (24);
CREATE INDEX idx_loc_temp_p12 ON time_series.loc_temp_p12(event_time);




INSERT INTO time_series.location_temp_p
                       ( event_time, event_hour, temp_celcius, location_id)
                       (SELECT event_time, extract(hour from event_time), temp_celcius, location_id
                        FROM time_series.location_temp);
						
-- without partition of the table				
Explain SELECT 
	location_id, AVG(temp_celcius)
FROM
	time_series.location_temp
WHERE
	event_time BETWEEN '2019-03-05' AND '2019-03-06'
Group by 
	location_id; 	
--Gather Merge  (cost=7636.11..7752.78 rows=1000 width=38)	


Explain SELECT 
	location_id, AVG(temp_celcius)
FROM
	time_series.location_temp_p -- cahngeiong the table
WHERE
	event_time BETWEEN '2019-03-05' AND '2019-03-06'
Group by 
	location_id;
-- Notice cost --   (cost=9197.72..9248.89 rows=200 width=38) -- Higher 


-- we partition on HOURS.. let's use it

Explain SELECT 
	location_id, AVG(temp_celcius)
FROM
	time_series.location_temp_p -- cahngeiong the table
WHERE
	event_hour BETWEEN 0 AND 4
Group by 
	location_id;
--  ->  Gather Merge  (cost=3698.20..3744.87 rows=400 width=38)	


	

  


	
	
	
