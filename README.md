# DB
CSV TO DB

### LOCAL VARIABLES:
DBPW (Database Password)

### webhook-marketing Table Instructions
1) Download the .csv from this list
https://www.klaviyo.com/list/U3Nuc6/marketing-properties-monitoring

2) Add file to the project (updated.csv)

3) Edit file to match all the required columns (Look for the base_file.xlsx)

4) Run the code

5) Test


### Delete duplicates

DELETE FROM klaviyo_hevo3.webhook_marketing AS a 
 USING (
        SELECT MAX(__hevo__ingested_at) AS ctid, 
			   id
          FROM klaviyo_hevo3.webhook_marketing
         GROUP BY id 
		HAVING COUNT(*) > 1) AS b
 WHERE a.__hevo__ingested_at <> b.ctid 
   AND a.id = b.id