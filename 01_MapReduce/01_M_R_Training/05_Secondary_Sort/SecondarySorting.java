package KNearestNeighbour;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Resources
// https://learning.oreilly.com/library/view/data-algorithms/9781491906170/ch01.html
// https://vangjee.wordpress.com/2012/03/20/secondary-sorting-aka-sorting-values-in-hadoops-mapreduce-programming-paradigm/
// https://pkghosh.wordpress.com/2011/04/13/map-reduce-secondary-sort-does-it-all/

public class SecondarySorting {

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

//        // In case writablecomparator interface is used with only this method no other thing in this class
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
    }

    /**
     * The partitioner decides which mapper’s output goes to which reducer based on the mapper’s output key.
     * For this, we need two plug-in classes: a custom partitioner to control which reducer processes which keys, and a custom Comparator to sort reducer values.
     * The custom partitioner ensures that all data with the same key  is sent to the same reducer.
     * The custom Comparator(Group comparator in our case) does sorting so that the key groups the data once it arrives at the reducer.
     */
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

    }

    /**
     * By default, each key maps to a separate group. Hadoop has default group comparator that gives values as list of
     * values inside the reducer but since our aim is to get another group of values, here we define our group comparator.
     *
     * In reducer class, we want to get the input as a group
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


}
