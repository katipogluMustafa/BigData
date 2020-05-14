package PearsonCorrelation;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RatingWritable implements WritableComparable<RatingWritable> {

    private int userId;
    private double rating;

    public RatingWritable(){}

    public RatingWritable(int userId, double rating){
        set(userId, rating);
    }

    public RatingWritable(Integer userId, Double rating){
        set(userId, rating);
    }

    public void set(int userId, double rating){
        this.userId = userId;
        this.rating = rating;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userId = in.readInt();
        rating = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.userId);
        out.writeDouble(this.rating);
    }

    public static RatingWritable read(DataInput in) throws IOException{
        RatingWritable r = new RatingWritable();
        r.readFields(in);
        return r;
    }

    /* Object Basics */
    @Override
    public String toString() {
        return userId + "," + rating;
    }

    @Override
    public int hashCode() {
        int hashUserId = Integer.hashCode(userId);
        int hashRating = Double.hashCode(rating);
        return (hashUserId * hashRating) % (hashUserId + hashRating);     // just for fun.d
    }

    @Override
    public boolean equals(Object otherObject) {
        if(this == otherObject) return true;
        if(otherObject == null) return false;
        if(getClass() != otherObject.getClass() ) return false;

        RatingWritable p = (RatingWritable) otherObject;

        return (this.userId == p.userId && this.rating == p.rating);
    }

    /* Getter Setters */

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    /* Comparable */
    @Override
    public int compareTo(RatingWritable p) {
        int result = Integer.compare(this.userId, p.userId);
        return (result != 0) ? result : Double.compare(this.rating, p.rating);
    }
}
