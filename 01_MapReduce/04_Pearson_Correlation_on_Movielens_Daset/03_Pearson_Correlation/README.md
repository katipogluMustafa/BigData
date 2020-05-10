<h1 align="center" >Pearson Correlation Step 3: </br>Pearson Correlation Coefficient Calculation</h1>

## NOTE!

I assume that you have followed the [Map Reduce 1-4 Notes](https://github.com/katipogluMustafa/BigData/tree/master/01_MapReduce).

## Defining Our Own Writable Data Types

When calculating pearson correlation, for each user and her/his each neighbour, we have rating data like `user1,rating neighbour1,rating` where ratings belong to the the same movie.

Instead of having the burden of moving around this data, I've come up with having my own `IntPairWritable` and `DoublePairWritable` data types so that i can store `user1,neighbour1` and their corresponding rating data `user1rating, neighbour1rating` so that they plug in to map reduce paradigm easily.

Here we see the actual parts of the new Writable data types. See [Here](https://github.com/katipogluMustafa/BigData/tree/master/01_MapReduce/03_Implementing_Our_Own_Writables) to understand the idea behind making own Writables and methods needed inside them.

````java
public class DoublePairWritable implements WritableComparable<DoublePairWritable> {

    private double first;
    private double second;

    // ... required methods
}
````

````java
public class IntPairWritable implements WritableComparable<IntPairWritable> {
    
    private int first;
    private int second;
    
    // ... required methods
}
````

## Pearson Mapper

Pearson mapper takes the input of step1 that is the movieRatings data which consists of </br> `movieId, map<RaterUserId, Rating>` data.

The aim of mapper is to generate all possible neighbour pairs and their corresponding rating to the same movies.

### Example

For example users {3, 7, 45, 124} watched the movie id of 1453 and gave rating in sequence {3.5, 2.5, 5.0, 2.0}.

Here we would like to generate pairs like `(3,7),(3.5,2.5)`  `(3,45),(3.5,5.0)` ...

Lets say user 3 and 7 watched 20 movies in common, later in reduce method the input key (3,7) will have 20 different rating data where each of them the ratings of these users to movies.

## Pearson Reducer

### Getting User Averages

Here we need to have average of each user, that is why in step2 we have calculated average rating of each user. Here in the setup method of the reducer, we read that data into a map so that we can use it in reduce method.


### Pearson Calculation

Pearson Calculation is calculated in exact same way that is found in the formula.

````java
int userId1 = userPair.getFirst();
int userId2 = userPair.getSecond();

double userId1Avg = avgRatings.get(userId1);
double userId2Avg = avgRatings.get(userId2);

double rating1, rating2;                      // userId1, rating1  - userId2, rating2
double temp1, temp2;

double sum = 0;
double denominatorSum1 = 0;
double denominatorSum2 = 0;
for(DoublePairWritable rating : ratings){
   rating1 = rating.getFirst();
   rating2 = rating.getSecond();
   temp1 = (rating1 - userId1Avg);
   temp2 = (rating2 - userId2Avg);
   sum += temp1 * temp2;
   denominatorSum1 += Math.pow(temp1, 2);
   denominatorSum2 += Math.pow(temp2, 2);
}

double pearsonCorr = sum / (Math.sqrt(denominatorSum1) * Math.sqrt(denominatorSum2));
````
