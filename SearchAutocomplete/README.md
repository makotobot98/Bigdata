
### Steps
##### 1. offline processing the language model in MapReduce and store it in MySQL for real time responsive query
**1a.** develop N-Gram library from the textfile scraped from Wikipidia
> e.g., "How are you doing today" will transform into 2,3,4 Gram phrases, 2-Gram be {How are, are you, you doing, ...}, 3-Gram be {How are you, are you doing, you doing today ...}. N-gram is the contiguous sequence of n items from a given sequence of texts.

**1b.** compute the conditional probability of each phrase within a sentence, develop language model which records the probability distribution over the entire sentences or texts

> e.g., given input phrase to be How are you _? and N-grams from N-Gram library built from 1a relating to input phrase "How are you _?" = {doing today, doing, like a cup of tea, ....}. Compare all conditional probability P(doing today | inputPhrase), P(like a cup of tea | inputPhrase), and filter the N-gram phrase so that P(X | How are you) that's top-k and store the top-k related N-Gram phrases into the data base where key = inputPhrase.

Above 1a and 1b uses two MapReduce jobs where job1 counts all occurances of all N-Gram(gram size = input, so if input = 4, then produces all 1, 2, 3, 4-Gram occurances). Job2 computes the probability and filtered out the highest probability inputPhrase + followingPhrase, output of Job2 writes to database directly

Improving performance:
* setted **threshold** t to ingnore phrases with frequency below t to improve the efficiency of shuffling. Size of t comes from command line input since t varies based on the size of input. used t = 20 in this project.
* Storing only the top k words with the highest probabilities to reduce spage usage.

##### 2. simple project set up using LAMP stack, online query from MySQL


starting_phrase | following_phrase | P
----------------|---------------|-------------
a | man | 264
a | little | 90
a | great | 83
a day | or | 14
a day | and | 6

then a simple using MySQL query can do the N-gram predicts N-gram:

```sql
SELECT * 
FROM languageModel
WHERE starting_phrase LIKE `userInput%`
ORDER BY P
LIMIT 10
```





