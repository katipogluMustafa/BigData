package Project.Pearson;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPairWritable implements WritableComparable<IntPairWritable> {
    
    private int first;
    private int second;

    public IntPairWritable(){}

    public IntPairWritable(int first, int second){
        set(first, second);
    }

    public IntPairWritable(Integer first, Integer second){
        set(first, second);
    }

    public void set(int first, int second){
        this.first = first;
        this.second = second;
    }

    /* Writable Methods */

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }

    public static IntPairWritable read(DataInput in) throws IOException{
        IntPairWritable r = new IntPairWritable();
        r.readFields(in);
        return r;
    }

    /* Object Basic Methods */

    @Override
    public int hashCode() {
        int hashFirst = Integer.hashCode(first);
        int hashSecond = Integer.hashCode(second);
        return (hashFirst * hashSecond) % (hashFirst + hashSecond);     // just for fun.d
    }

    @Override
    public boolean equals(Object otherObject) {
        if(this == otherObject) return true;
        if(otherObject == null) return false;
        if(getClass() != otherObject.getClass() ) return false;

        IntPairWritable p = (IntPairWritable) otherObject;

        return (p.first == this.first && p.second == this.second);
    }

    @Override
    public String toString() {
        return first + "," + second;
    }

    /* Getters */

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    /* Comparable */
    @Override
    public int compareTo(IntPairWritable p) {
        int result = Integer.compare(this.first, p.first);
        return (result != 0) ? result : Integer.compare(this.second, p.second);
    }
}
