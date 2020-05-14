# Implementing Writables

Here there are 2 options:
* Writable
* WritableComparable

Since we will be using our new data type as output of mapper, it needs to be comparable.(remember middle phase sorting in between map and reduce)

That's why I'll be using WritableComparable. But if I were to use the data type as output of reducer, it is okay to use Writable.


## Methods to Override for WritableComparable

````java
public void readFields(DataInput in) throws IOException {}
public void write(DataOutput out) throws IOException {}
public int compareTo(YourTypeT p) {}
````

## Methods I've Overridden as Extra

````java
// In case used in hash-map
public int hashCode() {}

// In case we compare RatingWritable types
public boolean equals(Object otherObject) {}

// In case we read the data from file(when calculating Pearson Correlation, we used this in later examples.)
public static RatingWritable read(DataInput in) throws IOException{}
````

## RatingWritable Walk-Through

````java
// Define the class as WritableComparable
public class RatingWritable implements WritableComparable<RatingWritable> {
    
    // In movieLens dataset there are users and their ratings
    // Here we want to store one user,rating pair in this RatingWritable Class
    
    private int userId;
    private double rating;

````

````java
    
    // Better to have one
    public RatingWritable(){}

    // Definitely needed.
    public RatingWritable(int userId, double rating){
        set(userId, rating);
    }
    
    // We may need it. [Optional]
    public RatingWritable(Integer userId, Double rating){
        set(userId, rating);
    }
    
    // We need it, in order to provide an interface that is same as classic Writable classes.
    public void set(int userId, double rating){
        this.userId = userId;
        this.rating = rating;
    }
````

````java
    // Read fields from input, part of WritableComparable interface
    @Override
    public void readFields(DataInput in) throws IOException {
        userId = in.readInt();
        rating = in.readDouble();
    }
    
    // Write to file, part of WritableComparable interface
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.userId);
        out.writeDouble(this.rating);
    }
    
    // Read from file, not part of anything, just like to have one
    public static RatingWritable read(DataInput in) throws IOException{
        RatingWritable r = new RatingWritable();
        r.readFields(in);
        return r;
    }

    /* Comparable */
    @Override
    public int compareTo(RatingWritable p) {
        int result = Integer.compare(this.userId, p.userId);
        return (result != 0) ? result : Double.compare(this.rating, p.rating);
    }
}

````

````java

    /* Object Basics Methods */
    
    // This one is used when writing to file, in order to format it.
    @Override
    public String toString() {
        return userId + "," + rating;
    }
     
    // In case we store the data in hash-map
    @Override
    public int hashCode() {
        int hashUserId = Integer.hashCode(userId);
        int hashRating = Double.hashCode(rating);
        return (hashUserId * hashRating) % (hashUserId + hashRating);     // just for fun.d
    }
    
    
    // This class not uses but in general we need to compare object , better to have one
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
````
