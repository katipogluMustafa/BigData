# Pearson Correlation Step 1: Calculate Movie Ratings

When calculating pearson correlation according to its formula, we need the rating of user u and user v on the same item i.

Here we create map for each item. The map gives rating data so that we can easily detect movies that are rated by both user u and v.

That is why here we create `movieId, hashmap<userId,rating>` data so that in pearson correlatin step, we can scan this map and for each pair of user, find pearson correlation because they have rated same movie so they have something in common.


## Input Format

MovieLens dataset ratings data.

````
userId, movieId, rating, timestamp
3, 1453, 3.5, 1554548
21, 1453, 2.5, 1554548
...
````

## Output Format

````
movieId, HashMap<userId, rating>
1453, {3:3.5, 21:2.5}         
````
