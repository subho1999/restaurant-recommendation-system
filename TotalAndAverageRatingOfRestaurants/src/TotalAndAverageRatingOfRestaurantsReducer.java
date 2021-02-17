import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TotalAndAverageRatingOfRestaurantsReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    private double totalSum = 0;
    private int totalCount = 0;
    private double average = 0;

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        for (DoubleWritable value: values) {
            totalSum += value.get();
            totalCount++;
        }
        average = totalSum / totalCount;
        context.write(new Text("Total Restaurants: "+totalCount), new Text("Average Rating: "+average));
        // context.write(key, new Text("Average Rating: "+average));
    }
}
