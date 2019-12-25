### 5 MapReduce Job to generate the MovieItemCF matrix, input format be (user_id, movie_id, rating).
#### Pipelines:
* **job1** - Group all movie:rating pair by the same user 

* **job2** - Generate the co-occurance matrix given input from job1 with the adjacency list of user(row), movie(column), rating(each cell)

* **job3** - Normalize the co-occurance matrix

* **job4** - Perform matrix multiplication between cooccurance matrix and rating matrix built from previous jobs. In this job **represented cooccurance matrix
in columnar representation** to do **matrix CELL multiplication in distributed fashion** 

* **job5** - Sum up the matrix cell multiplication from job4, between cooccurance matrix and rating matrix, and generate the actual each entry resulted from the multiplication

* **Driver** - taking cmd input/output path parameters



#### For more details in the implementation of ItemCF, please see the detailed comments in each of java file, I described how each job should be implemented in comments
  
