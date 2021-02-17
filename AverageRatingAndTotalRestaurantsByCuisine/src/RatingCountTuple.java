import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RatingCountTuple implements Writable {

    private int count = 0;
    private double rating = 0;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count);
        dataOutput.writeDouble(rating);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readInt();
        rating = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return count + " " + rating;
    }
}
