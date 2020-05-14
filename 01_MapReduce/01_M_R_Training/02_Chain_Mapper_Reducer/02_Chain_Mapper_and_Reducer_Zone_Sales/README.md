# Find Sales of Zone-1

By Using ChainMapper and ChainReducer, Filter Zone-1's sales.

## Input Format

ItemId SaleCount ZoneInfo space separated.

````
Item1 345 zone-1
Item4 855 zone-1
Item7 145 zone-1
Item1 234 zone-2
Item3 654 zone-2
Item2 231 zone-3
````

## Output Format

SaleCount, ItemId space separated.

````
345,Item1
855,Item4
145,Item7
````

## Notes

Using the ChainMapper and the ChainReducer classes it is possible to
compose Map/Reduce jobs that look like [MAP+ / REDUCE MAP*].

By using the predefined ChainReducer class in Hadoop you can chain multiple Mapper classes after a Reducer within the Reducer task.

For each record output produced by the Reducer, the Mapper classes are invoked in a chained fashion.

Special care has to be taken when creating chains that the key/values output by a Mapper are valid for the following Mapper in the chain.

### Benefits of Chained Map Reduce

* When MapReduce jobs are chained data from immediate mappers is kept in
memory rather than storing to disk so that another mapper in chain
doesn't have to read data from disk. Immediate benefit of this pattern
is a dramatic reduction in disk IO.

* Simpler tasks

## Reference

[ChainReducer Doc](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapred/lib/ChainReducer.html)

[Chaining Map Reduce](https://www.netjstech.com/2018/07/chaining-mapreduce-job-in-hadoop.html)
