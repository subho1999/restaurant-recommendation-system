import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;
import java.util.Map.Entry;

public class AverageRatingAndTotalRestaurantsByCuisineReducer extends Reducer<Text, RatingCountTuple, IntWritable, Text> {

    private TreeMap<Integer, Text> map = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<RatingCountTuple> values, Context context) throws IOException, InterruptedException {
        double ratingSum = 0;
        int count = 0;

        for (RatingCountTuple tuple : values) {
            ratingSum += tuple.getRating();
            count += tuple.getCount();
        }

        double avgRating = (double) ratingSum / count;
        map.put(count, new Text(key + " " + avgRating));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        while (!map.isEmpty()) {
            Entry<Integer, Text> entry = map.lastEntry();
            context.write(new IntWritable(entry.getKey()), entry.getValue());
            map.remove(entry.getKey());
        }
    }
}
