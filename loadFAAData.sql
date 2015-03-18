DROP TABLE s_wc;

CREATE TABLE IF NOT EXISTS s_wc (
  word  STRING, count INT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\s*';



LOAD DATA LOCAL INPATH 'hadoop-developer-course/data/shakespeare/input/all-shakespeare.txt'
INTO TABLE s_wc;



-- TOMI /data/shakespeare/input/all-shakespeare.txt
--
LOAD DATA INPATH '/data/shakespeare/input/all-shakespeare.txt'
INTO TABLE shakespeare_wc;




-- LOAD DATA INPATH '/data/shakespeare/input/all-shakespeare.txt'




---SELECT split(word,' ') FROM shakespeare_wc LIMIT 20;


SELECT explode(split(word,' ') ) AS count FROM shakespeare_wc LIMIT 20;


CREATE TABLE IF NOT EXISTS word_count_1 (
  word  STRING, count INT);
  
INSERT INTO table word_count_1
 SELECT explode(split(word,' ') ) , 1 FROM shakespeare_wc LIMIT 20; 

-- insert into table text_table select * from default.tab1;


split(string str, string pat)
   Split str around pat (pat is a regular expression) 
   
   
   
SELECT word, COUNT(*) 
FROM input LATERAL VIEW explode(split(text, ' ')) lTable as word 
GROUP BY word;

======


DROP TABLE FAAMaster;

CREATE TABLE FAAMaster (
  `N-NUMBER` STRING,
  `SERIAL NUMBER` STRING,
  `MFR MDL CODE` STRING,
  `ENG MFR MDL` STRING,
  `YEAR MFR` STRING,
  `TYPE REGISTRANT` STRING,
  `NAME` STRING,
  `STREET` STRING,
  `STREET2` STRING,
  `CITY` STRING,
  `STATE` STRING,
  `ZIP CODE` STRING,
  `REGION` STRING,
  `COUNTY` STRING,
  `COUNTRY` STRING,
  `LAST ACTION DATE` STRING,
  `CERT ISSUE DATE` STRING,
  `CERTIFICATION` STRING,
  `TYPE AIRCRAFT` STRING,
  `TYPE ENGINE` STRING,
  `STATUS CODE` STRING,
  `MODE S CODE` STRING,
  `FRACT OWNER` STRING,
  `AIR WORTH DATE` STRING,
  `OTHER NAMES(1)` STRING,
  `OTHER NAMES(2)` STRING,
  `OTHER NAMES(3)` STRING,
  `OTHER NAMES(4)` STRING,
  `OTHER NAMES(5)` STRING,
  `EXPIRATION DATE` STRING,
  `UNIQUE ID` STRING,
  `KIT MFR` STRING,
  `KIT MODEL` STRING,
  `MODE S CODE HEX` STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


!ls bootcampproj/FAA_Data_playground/Aircraft_Reg_DB_AR032015/Master.txt;


LOAD DATA 
LOCAL INPATH 'bootcampproj/FAA_Data_playground/Aircraft_Reg_DB_AR032015/Master.txt' 
OVERWRITE INTO TABLE FAAMaster;


ALTER TABLE FAAMaster CHANGE `MODE S CODE HEX` MODE_S_CODE_HEX STRING;


SELECT * FROM FAAMaster limit 30;

SELECT MODE_S_CODE_HEX  FROM FAAMaster limit 30;

-- does not find it
SELECT MODE_S_CODE_HEX FROM FAAMaster  WHERE MODE_S_CODE_HEX = 'A00001%'; 

SELECT MODE_S_CODE_HEX FROM FAAMaster  WHERE MODE_S_CODE_HEX LIKE 'A00%'; 

-- after rename to col without blank it does find it
SELECT `MODE S CODE HEX` AS HEXID FROM  FAAMaster limit 30;
SELECT `MODE S CODE HEX` AS HEXID FROM FAAMaster  WHERE HEXID = 'A00001';



SELECT  * FROM FAAMaster 
WHERE `MODE S CODE HEX` = 'A8F96E';

SELECT * FROM FAAMaster WHERE `MODE S CODE HEX` = 'A8F96E';

SELECT * FROM FAAMaster WHERE `MODE S CODE HEX` = 'A8F96E';



N-NUMBER	SERIAL NUMBER	MFR MDL CODE	

ENG MFR MDL	YEAR MFR	TYPE REGISTRANT	NAME	STREET	STREET2	CITY	STATE	ZIP CODE	REGION	COUNTY	COUNTRY	LAST ACTION DATE	CERT ISSUE DATE	CERTIFICATION	TYPE AIRCRAFT	TYPE ENGINE	STATUS CODE	MODE S CODE	FRACT OWNER	AIR WORTH DATE	OTHER NAMES(1)	OTHER NAMES(2)	OTHER NAMES(3)	OTHER NAMES(4)	OTHER NAMES(5)	EXPIRATION DATE	UNIQUE ID	KIT MFR	 KIT MODEL	MODE S CODE HEX
1    	1071                          	3980115	

54556	1988	5	

FEDERAL AVIATION ADMINISTRATION                   	WASHINGTON REAGAN NATIONAL ARPT  	
3201 THOMAS AVE HANGAR 

6         	WASHINGTON        	DC	20001     	

1	001	US	20131115	

19900214	1T        	5	5 	V 	50000001	 	19880909	                                                  	                                                  	                                                  	                                                  	                                                  	20161130	00524101	                              	                    	A00001    


