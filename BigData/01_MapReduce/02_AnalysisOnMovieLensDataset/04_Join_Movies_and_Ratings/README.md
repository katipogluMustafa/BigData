# Join Movies and Ratings

Here we want to join the ratings.csv file and movies.csv file.

## Type of Join

There are two types of joins in hadoop.

1. Map Join
2. Reduce Join

Here we use Map join since we have multiple input files.

## So how we join in hadoop with map join?

* Write Mapper for each input file
  * Key is the attribute we want to join.
  * Values are the extra attributes of the tables.
  * Here each mapper has to have the same key.

  * In order to differentiate tables, I use table number in front of their values and in reduce method combine according to that table number so that we always keep the order as first table comes first, second comes after.
 
* Associate each mapper with input
* In the reduce method join the data using the key

## Output Format

output contains format of
````
key table1 \ttable2 \ttable2 \table2.....
````

* Here 
  * Key and table1 has always one-to-one relation.
  * Key and table2 has always one-to-may relation.

That is why we use \t in front of them.

In this specific movielens dataset case output format is

````
movieId movieTitle \tuserId,rating,ts \tuserId,rating,ts \tuserId,rating,ts ....
````

Here each movie has multiple ratings data so we use \t to distungish.
