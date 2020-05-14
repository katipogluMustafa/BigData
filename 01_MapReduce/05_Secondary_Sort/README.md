# Secondary Sort

Secondary Sort is used whenever we need to group the data that is coming from mapper to reducer in a different way.

Secondary sort is also used to have sorted list of data.

Here we have correlations in between users that has been calculated in the previous notes and we want to get sorted list of them.

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
## Code Analysis

Here we need to first define our own Partitioner and Grouping Comparators so that we can override hadoop's default behaviour.

### Our Custom Data Class

Before everything we need to do in order to make things simple, lets just begin by defining our own custom class that stores the data we want to sort so that in the next steps we can use this class to compare the data we have.

We have input data in the form of -> `userId1, userId2, correlationInBetweenThem`

So here, as we just explained in the [Implementing our own writable classes](https://github.com/katipogluMustafa/BigData/tree/master/01_MapReduce/03_Implementing_Our_Own_Writables) notes, we define a writable custom class.

````java
public static class UserPairPearsonCorr implements Writable {

        private int userId1;                     // Natural Key For Sorting in KNN
        private int userId2;
        private double corr;                    // Pearson Correlation, Secondary Key For Sorting

        public UserPairPearsonCorr(){}

        public UserPairPearsonCorr(int userId1, int userId2, double corr){
            set(userId1, userId2, corr);
        }

        public void set(int userId1, int userId2, double corr){
            this.userId1 = userId1;
            this.userId2 = userId2;
            this.corr = corr;
        }
                /* Writable Methods */

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(this.userId1);
            out.writeInt(this.userId2);
            out.writeDouble(this.corr);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.userId1 = in.readInt();
            this.userId2 = in.readInt();
            this.corr = in.readDouble();
        }

        public static UserPairPearsonCorr read(DataInput in) throws IOException{
            UserPairPearsonCorr u = new UserPairPearsonCorr();
            u.readFields(in);
            return u;
        }

        /* Object Basics */

        @Override
        public boolean equals(Object otherObject) {
            if(this == otherObject) return true;
            if(otherObject == null) return false;
            if(getClass() != otherObject.getClass() ) return false;

            UserPairPearsonCorr p = (UserPairPearsonCorr) otherObject;

            return (this.userId1 == p.userId1) && (this.userId2 == p.userId2) && (this.corr == p.corr);
        }

        @Override
        public String toString() {
            return this.userId1 + "," + this.userId2 + "," + this.corr;
        }

        /* Getters and Setters */

        public int getUserId1() {
            return userId1;
        }

        public void setUserId1(int userId1) {
            this.userId1 = userId1;
        }

        public int getUserId2() {
            return userId2;
        }

        public void setUserId2(int userId2) {
            this.userId2 = userId2;
        }

        public double getCorr() {
            return corr;
        }

        public void setCorr(double corr) {
            this.corr = corr;
        }
````

#### Enhancing Writable Data Class For Sorting

Remember, here we want to do secondary sort. As we all know in order to sort the data, we need to be able to compare these data that we have in our hands. That is why here we need to implement WritableComparable and provide a compareTo method instead of just Writable.

````java
        /**
         * Since UserPairPearsonCorr class is all about Secondary Sort, we need to be able to compare objects.
         * Here We want to compare in a way that when both Pair has the same userId1 then compare their
         * pearson correlations so that at the end of the whole calculations we can get the correlations sorted
         * for each userId1.
         */
        @Override
        public int compareTo(UserPairPearsonCorr u){
            if(u == null)
                return Integer.MIN_VALUE;                           // Put the non ones as smallest

            // For ascending sorting return itself,
            // For descending return as negative.

            int compareValue = Integer.compare(this.userId1, u.userId1);
            if(compareValue != 0)
                return -compareValue;                             // if -compareValue, then for Descending Sorting

            return -Double.compare(this.corr, u.corr);           // if -Double.compare(), then for Descending Sorting
        }
````

### Enhanced Version of the Code


````java
    /**
     * We define a proper data structure for holding our key and value, while also providing the sort order of intermediate keys.
     * UserPairPearsonCorr is used for holding our key and value.
     * By using compareTo method we give the sort order, all other methods are part of my design, not a necessity
     */
    public static class UserPairPearsonCorr implements WritableComparable<UserPairPearsonCorr> {

        private int userId1;                     // Natural Key For Sorting in KNN
        private int userId2;
        private double corr;                    // Pearson Correlation, Secondary Key For Sorting

        public UserPairPearsonCorr(){}

        public UserPairPearsonCorr(int userId1, int userId2, double corr){
            set(userId1, userId2, corr);
        }

        public void set(int userId1, int userId2, double corr){
            this.userId1 = userId1;
            this.userId2 = userId2;
            this.corr = corr;
        }

        /* Comparable Methods */

        /**
         * Since UserPairPearsonCorr class is all about Secondary Sort, we need to be able to compare objects.
         * Here We want to compare in a way that when both Pair has the same userId1 then compare their
         * pearson correlations so that at the end of the whole calculations we can get the correlations sorted
         * for each userId1.
         */
        @Override
        public int compareTo(UserPairPearsonCorr u){
            if(u == null)
                return Integer.MIN_VALUE;                           // Put the non ones as smallest

            // For ascending sorting return itself,
            // For descending return as negative.

            int compareValue = Integer.compare(this.userId1, u.userId1);
            if(compareValue != 0)
                return -compareValue;                             // if -compareValue, then for Descending Sorting

            return -Double.compare(this.corr, u.corr);           // if -Double.compare(), then for Descending Sorting
        }

        /* Writable Methods */

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(this.userId1);
            out.writeInt(this.userId2);
            out.writeDouble(this.corr);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.userId1 = in.readInt();
            this.userId2 = in.readInt();
            this.corr = in.readDouble();
        }

        public static UserPairPearsonCorr read(DataInput in) throws IOException{
            UserPairPearsonCorr u = new UserPairPearsonCorr();
            u.readFields(in);
            return u;
        }

        /* Object Basics */

        @Override
        public boolean equals(Object otherObject) {
            if(this == otherObject) return true;
            if(otherObject == null) return false;
            if(getClass() != otherObject.getClass() ) return false;

            UserPairPearsonCorr p = (UserPairPearsonCorr) otherObject;

            return (this.userId1 == p.userId1) && (this.userId2 == p.userId2) && (this.corr == p.corr);
        }

        @Override
        public String toString() {
            return this.userId1 + "," + this.userId2 + "," + this.corr;
        }

        /* Getters and Setters */

        public int getUserId1() {
            return userId1;
        }

        public void setUserId1(int userId1) {
            this.userId1 = userId1;
        }

        public int getUserId2() {
            return userId2;
        }

        public void setUserId2(int userId2) {
            this.userId2 = userId2;
        }

        public double getCorr() {
            return corr;
        }

        public void setCorr(double corr) {
            this.corr = corr;
        }
      }
````

#### Alternative

Instead of having big bulky class like the one we have just defined, you would have defined the same class with only one compare method by implementing WritableComparator<> generic class.

As you can see in this method you get the input as text and parse it here. But here as you can see this method does more than one thing which is not appropriate way of writing object oriented code. 

Since I want to distribute the work load as much as possible, I have parsed the data inside of the mapper class and used the parsed data to create my own writable data class.

````java

//        // implement writablecomparator interface
//        @Override
//        public int compare(WritableComparable a, WritableComparable b) {
//            Text t1 = (Text)a;
//            Text t2 = (Text)b;
//
//            // Format-> userId1,userId2,pearsonCorrelation
//            String[] tokens1 = t1.toString().split(",");
//            String[] tokens2 = t2.toString().split(",");
//
//            int uid1_1 = Integer.parseInt(tokens1[0]);
//            int uid2_1 = Integer.parseInt(tokens2[0]);
//
//            int uid_1_result = Integer.compare(uid1_1,uid2_1);
//
//            if( uid_1_result != 0 )
//                return uid_1_result;
//
//            double corr1 = Integer.parseInt(tokens1[2]);
//            double corr2 = Integer.parseInt(tokens2[2]);
//
//            return Double.compare(corr1, corr2);
//        }
````

### Custom Partitioner

The partitioner decides which mapper’s output goes to which reducer based on the mapper’s output key.
* For this reason, we need two plug-in classes: a custom partitioner to control which reducer processes which keys, and a custom Comparator to sort reducer values.
* The custom partitioner ensures that all data with the same key  is sent to the same reducer. [1]
* The custom Comparator(Group comparator in our case) does sorting so that the key groups the data once it arrives at the reducer.

Here I have implemented Configurable as an extra in case we need to get configuration from our driver class but we could have only getPartition method and nothing else in this classes code and still work great.

````java
    public static class UserPairPearsonCorrPartitioner extends Partitioner<UserPairPearsonCorr, Text> implements Configurable {

        // In this class I have not needed conf yet. But for those who may need, need to have this one.
        private Configuration conf;

        @Override
        public int getPartition(UserPairPearsonCorr userPairPearsonCorr, Text text, int numPartitions) {
            // Make sure partitions are non-negative.
            return Math.abs(Integer.hashCode(userPairPearsonCorr.userId1) % numPartitions);
        }

        @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
        }

        @Override
        public Configuration getConf() {
            return conf;
        }
````

### Custom Group Comparator

Hadoop's default group comparator that gives values as list of values inside the reducer but since our aim is to get another group of values, here we define our group comparator.


````java
    /*
     * By default, each key maps to a separate group. 
     * In reducer class, we want to get the input as a group in different style.
     * In our case we want key: UserId value:all neighbours as list
     * So here we need to define a group comparator that would allow us to have a list of neighbours as value in reducer
     */
    public static class UserPairPearsonCorrGroupingComparator extends WritableComparator{

        public UserPairPearsonCorrGroupingComparator(){
            super(UserPairPearsonCorr.class, true);
        }

        /**
         * This comparator controls which keys are grouped
         * together into a single call to the reduce() method.
         */
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            UserPairPearsonCorr pair = (UserPairPearsonCorr) a;
            UserPairPearsonCorr pair2 = (UserPairPearsonCorr) b;

            return Integer.compare(pair.userId1, pair2.userId1);
        }
    }

````

### Map-Reduce

Here we need to set our own partitioner and group comparator inside of our driver class

* Map class just parses data and sends values as pair as well.
  * The reason for this is that I want to print sorted pairs so that we see them sorted but here we could have just returned 'corr'

* Reduce class prints non-nan data as text.

````java

public class KNearestNeighbour extends Configured implements Tool {

    public static class SplitMapper extends Mapper<LongWritable, Text, UserPairPearsonCorr, UserPairPearsonCorr>{
        public static UserPairPearsonCorr pair = new UserPairPearsonCorr();

        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            // Line Format -> userId1, userId2, pearsonCorrelation
            String[] tokens = line.toString().split(",");
            pair.setUserId1(Integer.parseInt(tokens[0]));
            pair.setUserId2(Integer.parseInt(tokens[1]));
            double corr = Double.parseDouble(tokens[2]);
            pair.setCorr(corr);

            // See, both pair contains 'corr' and also our value is the 'corr'[For Secondary Sorting Purposes]
            context.write(pair, pair);
        }
    }

    public static class KNNReducer extends Reducer<UserPairPearsonCorr, UserPairPearsonCorr, Text, Text>{

        @Override
        protected void reduce(UserPairPearsonCorr pairs, Iterable<UserPairPearsonCorr> corrs, Context context) throws IOException, InterruptedException {
            StringBuilder sortedCorrList = new StringBuilder("\n");
            for(UserPairPearsonCorr corr: corrs)
                if(!Double.isNaN(corr.getCorr()))                           // Do not print Nan values
                    sortedCorrList.append(corr.toString()).append("\n");

            context.write(new Text(pairs.getUserId1() + "-->"), new Text(sortedCorrList.append("\n\n").toString()));
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        String iPath = "hdfs://master:9000/user/herolenk/outputs/pearsonCorr/part-r-00000";
        String oPath = "hdfs://master:9000/user/herolenk/outputs/knn";

        Path inputPath = new Path(iPath);
        Path outputPath = new Path(oPath);

        Configuration configuration = new Configuration();
        configuration.set("mapred.textoutputformat.separator", ",");

        Job job = Job.getInstance(configuration);

        job.setJarByClass(KNearestNeighbour.class);
        job.setPartitionerClass(UserPairPearsonCorrPartitioner.class);               // my own partitioner
        job.setGroupingComparatorClass(UserPairPearsonCorrGroupingComparator.class); // my own group comparator

        job.setMapperClass(SplitMapper.class);
        job.setReducerClass(KNNReducer.class);

        job.setMapOutputKeyClass(UserPairPearsonCorr.class);
        job.setMapOutputValueClass(UserPairPearsonCorr.class);

        job.setOutputKeyClass(UserPairPearsonCorr.class);
        job.setOutputValueClass(Text.class);

        // For debugging purposes, overwrite the old output
        outputPath.getFileSystem(configuration).delete(outputPath, true);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int errCode = -1;

        try{
            errCode = ToolRunner.run(new Configuration(), new KNearestNeighbour(), args);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.exit(errCode);
    }
}

````

## Resources

[[1] Secondary Sort in Hadoop Implemented[This one best]](https://learning.oreilly.com/library/view/data-algorithms/9781491906170/ch01.html)

[[2] Secondary Sort](https://vangjee.wordpress.com/2012/03/20/secondary-sorting-aka-sorting-values-in-hadoops-mapreduce-programming-paradigm)

[[3] Secondary Sort](https://pkghosh.wordpress.com/2011/04/13/map-reduce-secondary-sort-does-it-all/)
