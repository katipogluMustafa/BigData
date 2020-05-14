package Project.Pearson;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DoublePairWritable implements WritableComparable<DoublePairWritable> {

    private double first;
    private double second;

    public DoublePairWritable(){}

    public DoublePairWritable(double first, double second){
        set(first, second);
    }

    public DoublePairWritable(Double first, Double second){
        set(first, second);
    }

    public void set(double first, double second){
        this.first = first;
        this.second = second;
    }

    /* Writable Methods */

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(first);
        out.writeDouble(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readDouble();
        second = in.readDouble();
    }

    /* Object Basic Methods */

    @Override
    public int hashCode() {
        int hashFirst = Double.hashCode(first);
        int hashSecond = Double.hashCode(second);
        return (hashFirst * hashSecond) % (hashFirst + hashSecond);     // just for fun.d
    }

    @Override
    public boolean equals(Object otherObject) {
        if(this == otherObject) return true;
        if(otherObject == null) return false;
        if(getClass() != otherObject.getClass() ) return false;

        DoublePairWritable p = (DoublePairWritable) otherObject;

        return (p.first == this.first && p.second == this.second);
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }

    /* Getters */

    public double getFirst() {
        return first;
    }

    public void setFirst(double first) {
        this.first = first;
    }

    public double getSecond() {
        return second;
    }

    public void setSecond(double second) {
        this.second = second;
    }

    /* Comparable */
    @Override
    public int compareTo(DoublePairWritable p) {
        int result = Double.compare(this.first, p.first);
        return (result != 0) ? result : Double.compare(this.second, p.second);
    }
}
