### Steps:
1. converting raw data into DS/DF

2. self join on user_id
    result:
        user_id | movie_id1 | movie_rating2 | movie_id2 | movie_rating2
    - here is necessary to filter out the duplicates meaning (1, starWar, 3.5, transformers, 4) and (1, transformers, 4, starWar, 3.5). we do this by preserve joined row with condition movie_id1 < movie_id2
3. we want to group same (movie_id1, movie_id2), which is each entry of co-occurance matrix. so groupBy(movie_id1, movie_id2), on each group, 
4. perform cosine similarity computation
    -  https://en.wikipedia.org/wiki/Cosine_similarity
    -  output format for each row should be: (movie_id1, movie_id2, similarity_score), where movie_id1 < movie_id2 due to our previous join condition
5. Save reuslts into the database, or writing to a file. Within an application, we can take following appoaches to getr the similarity between two movie
    a. write to JSON. And use cron to schedule jobs by day, when needed, go the specific file location partitioned by date to fetch the results
    b. write to database such as MySQL. Use cron to schedule jobs by day. This approach is faster in real-time querying, but not so necessary since it’s not an auto-complete application where we need results in mille seconds
