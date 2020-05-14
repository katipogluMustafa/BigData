# Secondary Sort

Secondary Sort is used whenever we need to group the data that is coming from mapper to reducer in a different way.

Secondary sort is also used to have sorted list of data.

Here we need have correlations in between users that has been calculated in the previos notes and we want to get sorted list of them.

## Input

Input format consists of correlation between every user and every other user.

````
userId1,userId2,corr
1,2,0.87
1,3,0,96
1,4,-0,23
...
2,1,0.87
2,3,0,80
2,4,0.96
````

## Output

````
userId1->,
userId1,userId2,corr
````

````
1->,
1,3,0.96
1,2,0.87
1,47,0.83
....
2->,
2,4,0.96
2,1,0.87
2,3,0.80

````
