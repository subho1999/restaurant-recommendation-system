import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RestaurantSentimentAnalysisReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String restaurantName = "";
        int posCount = 0;
        int negCount = 0;

        for (Text value : values) {
            String[] val = value.toString().split(" ");
            if (val[0].equalsIgnoreCase("A")) {
                restaurantName = value.toString().substring(2);
            }
            if (value.toString().equalsIgnoreCase("Positive Review")) {
                posCount++;
            }
            if (value.toString().equalsIgnoreCase("Negative Review")) {
                negCount++;
            }
        }
        if (posCount > 0 || negCount > 0) {
            if (restaurantName.length() > 0) {
                context.write(new Text(restaurantName), new Text(posCount + "\t" + negCount));
            }
        }
    }
}
