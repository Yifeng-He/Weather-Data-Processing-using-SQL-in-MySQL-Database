-- This sql script is used to process the weather data by reading the input from a csv file and then storing the result into a csv file for each processing task.

-- create a database
CREATE DATABASE weather1;
USE weather1;

-- create a table to store the input data
CREATE TABLE data1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
stationcode VARCHAR(20),
station VARCHAR(30),
datefield VARCHAR(30),
prcp DOUBLE(10,3),
tmax DOUBLE(10,3),
tmin DOUBLE(10,3) );

-- load data from local csv file
-- format in csv file: STATION,STATION_NAME,DATE,PRCP,TMAX,TMIN
-- GHCND:NLE00109300,STAVENISSE NL,19800101,53,-9999,-9999

LOAD DATA LOCAL INFILE 'C:\\data\\data.csv' INTO TABLE data1 
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (stationcode, station, datefield, prcp, tmax, tmin);


-- *** Task 1: order tha table based on day max temperature
-- create a temporary table
CREATE TABLE temp1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
station VARCHAR(30),
datefield VARCHAR(30),
tmax DOUBLE(10,3));

-- insert data into table temp1 by filtering out the rows with missing values
INSERT INTO temp1(station, datefield, tmax)  
SELECT station, datefield, tmax FROM data1 WHERE tmax > -9999.0 ORDER BY tmax DESC;

-- store the result into a csv file
-- we must change secure-file-priv="C:/ProgramData/MySQL/MySQL Server 5.7/Uploads" in "C:\ProgramData\MySQL\MySQL Server 5.7\my.ini" to secure-file-priv="" by using Notepad as admin.
 
SELECT * INTO OUTFILE 'C:\\data\\task1_result.csv'
    FIELDS TERMINATED BY ',' 
    LINES TERMINATED BY '\n'
FROM temp1;

-- drop the temporary tables
DROP TABLE temp1;


-- *** Task 2: for each station, find the maximun of day max temperature for each year 

-- create a temporary table
CREATE TABLE temp2 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
station VARCHAR(30),
year VARCHAR(30),
tmax DOUBLE(10,3));

-- insert data into table temp2 
INSERT INTO temp2 (station, year, tmax)  
SELECT station, substring(datefield,1,4), tmax FROM data1 WHERE tmax > -9999;

-- create the result table
CREATE TABLE temp2_result (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
station VARCHAR(30),
year VARCHAR(30),
tmaxOfYear DOUBLE(10,3));

-- transform the data into temp2_result 
INSERT INTO temp2_result (station, year, tmaxOfYear)  
SELECT station, year, MAX(tmax) from temp2 GROUP BY station, year;

-- store the result into a csv file
SELECT * INTO OUTFILE 'C:\\data\\task2_result.csv'
    FIELDS TERMINATED BY ',' 
    LINES TERMINATED BY '\n'
FROM temp2_result;

-- drop the temporary tables
DROP TABLE temp2, temp2_result;


-- *** Task 3: for each station, find the average of day max  temperatures per year 

-- create a temporary table
CREATE TABLE temp3 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
station VARCHAR(30),
year VARCHAR(30),
tmax DOUBLE(10,3));

-- transform the data into temp3
INSERT INTO temp3(station, year, tmax)  
SELECT station, substring(datefield,1,4), tmax FROM data1 WHERE tmax > -9999;

-- create the result table
CREATE TABLE temp3_result (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
station VARCHAR(30),
year VARCHAR(30),
AvgOfTMaxPerYear DOUBLE(10,3));

-- transform the data into temp3_result
INSERT INTO temp3_result(station, year, AvgOfTMaxPerYear)  
SELECT station, year, avg(tmax) FROM temp3 GROUP BY station, year;

-- store the result into csv file
SELECT * INTO OUTFILE 'C:\\data\\task3_result.csv'
    FIELDS TERMINATED BY ',' 
    LINES TERMINATED BY '\n'
FROM temp3_result;

-- drop the temporary tables
DROP TABLES temp3, temp3_result;


-- *** Task 4: for each station, find the year with the maximum average day temperature gap (day temperature gap = tMax - tMin)
-- create a temporary table
CREATE TABLE temp4 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
station VARCHAR(30),
year VARCHAR(30),
tmax DOUBLE(10,3),
tmin DOUBLE(10,3) );

-- insert the data into temp4
INSERT INTO temp4(station, year, tmax, tmin)  
SELECT station, substring(datefield,1,4), tmax, tmin FROM data1 WHERE tmax > -9999.0 AND tmin > -9999.0;

-- create table temp4_1 for day temperature gap
CREATE TABLE temp4_1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
station VARCHAR(30),
year VARCHAR(30),
tgap DOUBLE(10,3));

-- insert the data into temp4_1
INSERT INTO temp4_1(station, year, tgap)  
SELECT station, year, (tmax-tmin) FROM temp4;

-- create table temp4_2 to store the maximum day temperature gap for each year
CREATE TABLE temp4_2 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
station VARCHAR(30),
year VARCHAR(30),
maxTGap DOUBLE(10,3));

-- insert the data into temp4_2
INSERT INTO temp4_2(station, year, maxTGap)  
SELECT station, year, MAX(tgap) as maxTGap FROM temp4_1 GROUP BY station, year;

-- create table temp4_3 to store the year with maximum day temperature gap for each station
CREATE TABLE temp4_3 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
station VARCHAR(30),
maxTGapInHistory DOUBLE(10,3));

-- insert data into temp4_3
INSERT INTO temp4_3(station, maxTGapInHistory)  
SELECT station, MAX(maxTGap) as maxTGapInHistory FROM temp4_2 GROUP BY station;

-- create the table to store the output result
CREATE TABLE temp4_result (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
station VARCHAR(30),
year VARCHAR(30),
maxTGapInHistory DOUBLE(10,3));

-- insert the data into temp4_result using the information from two tables
INSERT INTO temp4_result (station, year, maxTGapInHistory)  
SELECT w2.station, w1.year, w2.maxTGapInHistory FROM temp4_2 w1 JOIN temp4_3 w2 ON w1.maxTGap = w2.maxTGapInHistory;

-- store the result into a csv file
SELECT * INTO OUTFILE 'C:\\data\\task4_results.csv'
    FIELDS TERMINATED BY ',' 
    LINES TERMINATED BY '\n'
FROM temp4_result;

-- drop the temporary tables
DROP TABLE temp4, temp4_1, temp4_2, temp4_3, temp4_result;


-- *** Task 5: find the yearly average of the day max temperatures for the specific station "VITORIA"

-- create table temp5 to store the day max temperatures
CREATE TABLE temp5 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
station VARCHAR(30),
year VARCHAR(30),
tmax DOUBLE(10,3));

-- insert data into temp5
INSERT INTO temp5(station, year, tmax)  
SELECT station, substring(datefield,1,4), tmax FROM data1 WHERE tmax > -9999.0 AND station like "%VITORIA%";

-- create table temp5_result to store the avregae of day max temperatures

CREATE TABLE temp5_result (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
station VARCHAR(30),
year VARCHAR(30),
avg_tmax DOUBLE(10,3));

-- insert the data into temp5_result 
INSERT INTO temp5_result (station, year, avg_tmax )  
SELECT station, year, AVG(tmax) FROM temp5 GROUP BY station, year;

-- store the result into a csv file
SELECT * INTO OUTFILE 'C:\\data\\task5_results.csv'
    FIELDS TERMINATED BY ',' 
    LINES TERMINATED BY '\n'
FROM temp5_result;

-- drop the temporary tables
DROP TABLE temp5, temp5_result;

-- *********************************************
-- done all tasks, drop the database
DROP DATABASE weather1;








 







