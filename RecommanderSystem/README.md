# Movie Recommander System with input size of 100K as well as 1Million records
 ### steps: 
1. read (userID, (movieID, movieRating)) 
2. self join and deduplicate to find the co occurance movie records rated by same user
3. Group all same movie pairs rated by different users (so group all (movie1, movie2) rated by user1, 2, 3 ...) 		
        we are garanteed the dimension of vectorization of movie1,2 are same, so we can compute cosine similarity
4. compute cosine similarity between each movie pair
5. cache (or push into the database or file system) similarity between each pair of movie on memory
6. take input from command line about which movie to query to get the similar movie recommandation
7. print the result in console

### 1M data records
for 1M data records, see input data ml-1m. 
* uses default SparkConf - this way, use the defaults EMR sets up 
* 1M data records are stored and refered from Amazon S3 when running the job on EMR
* setted partition size to be 100, since using **partitionBy()** which benefits a lot from partitioning

### results
* 10K data records are processed in 5 seconds
* 1m data records are processed in around 4 minutes
