import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NameNegativeReviewTuple implements Writable {

    private String restaurantName;
    private int negativeCount;

    public String getRestaurantName() {
        return restaurantName;
    }

    public void setRestaurantName(String restaurantName) {
        this.restaurantName = restaurantName;
    }

    public int getNegativeCount() {
        return negativeCount;
    }

    public void setNegativeCount(int negativeCount) {
        this.negativeCount = negativeCount;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(restaurantName);
        dataOutput.writeInt(negativeCount);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        restaurantName = dataInput.readUTF();
        negativeCount = dataInput.readInt();
    }
}
