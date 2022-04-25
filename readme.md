-------------------Take Home Challenge Readme File-----------------

#Steps to deploy he pipeline in productions
(1.) Define a AWS Glue Crawler which will establish the connection with our different source systems, and craw through the our both main table (de_event_op, user_level_info) saved in source system.
	  * It will create AWS glue data catalog and for both of our table ( as "raw_table_event" and "raw_table_user")  and will maintain a Glue database catalog ("raw_db") for them.
	  * We can run the crawler on a fixed schedule or on-demand as per the need.
	  * We can simply query on these two raw layer tables also using AWS Athena.
	  
(2.) Create a glue job which using 'ETL_job.py' script.
	  * This job will take the data from glue catalog tables and apply needed transformation.
	  * After transforming the data, it will save the tables s3 location in parquet format for security reasons.
	  * It will also create Glue data catalog in different Glue Database ("Pre_db"), so that we can query the data in athena.
	  
(3.) Save the glue job
	  * Glue job also can be schedule to run on a fixed schedule.

Main Challenging Part of the assessment
(4.) The main challenge in all 3 task given was to combine both tables and apply the join in such a way so that we have a new table in which we will be having columns ( User_id, Partner_id, Unlock (from 'user_level_info') and their corresponding values stored of column 'utm_campaign' and 'utm_medium' in 'de_event_op' table.

(5.) It was challenging becuase for applying the joins on these two tables was not simple as 'user_id' was save in a array_column 'unique_users_installed' in table 'de_event_op'.

---End---
