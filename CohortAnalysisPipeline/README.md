### Compute daily the average bike duration of each user, total subscription of users, and 1,3,7-day retention

**In total of 5 processes**

1. process that compute/update a global user list (user_id, first_subscription_timestamp)
2. process that compute everyday average bike duration of each user
3. process that compute the 1-day retention
4. process that compute the 3-day retention
5. process that compute the 5-day retention

---

**For retention computation, say if we are computing 7-day retention, we wait until we have previous 7-day data, we then go back compute 7-day retention and overwrite to the original data. Same thing for 1-day, and 3-day retention computation. See example workflow below**

---

**For implementation detail, please find it at each file, I have commented most of the workflow**

---


**All Job runs on GCP, including storage, cluster, airflow integration with cloud composer**

---


### Example of a 1-day pipeline workflow:
on day 11/08/2019, suppose we have bike trip data and global unique user list collected from 11/01 - 11/07

1. compute/update the global unique user list by adding today(11/08)'s new user, overwrite to the original unique user list 

2. compute the average bike duration for each user => overwrite to data 11/08/19

3. compute 7-day retention since 11/08 - 11/01 = 7, data in 11/01 are "ripe" enough output(overwrite) result to 11/01 

4. compute 3-day retention since 11/08 - 11/05 = 3, data in 11/05 are "ripe" enough output(overwrite) result to 11/05

5. compute 7-day retention since 11/08 - 11/07 = 1, data in 11/07 are "ripe" enough output(overwrite) result to 11/01  

---

Airflow Scheduling DAG, (managed using **Google Cloud Composer**)


* DAG

![alt text](https://github.com/makotobot98/Bigdata/blob/master/CohortAnalysisPipeline/airflow/DAG.png)

* Graph View
![alt text](https://github.com/makotobot98/Bigdata/blob/master/CohortAnalysisPipeline/airflow/DAGGraph.png)



