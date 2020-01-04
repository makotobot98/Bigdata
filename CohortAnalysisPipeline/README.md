Example of a 1-day pipeline workflow:
on day 11/08/2019, suppose we have bike trip data and global unique user list collected from 11/01 - 11/07

1. compute/update the global unique user list by adding today(11/08)'s new user, overwrite to the original unique user list 

2. compute the average bike duration for each user => overwrite to data 11/08/19

3. compute 7-day retention since 11/08 - 11/01 = 7, data in 11/01 are "ripe" enough output(overwrite) result to 11/01 

4. compute 3-day retention since 11/08 - 11/05 = 3, data in 11/05 are "ripe" enough output(overwrite) result to 11/05

5. compute 7-day retention since 11/08 - 11/07 = 1, data in 11/07 are "ripe" enough output(overwrite) result to 11/01  

Airflow Scheduling DAG



