import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class Top10RestaurantsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private TreeMap<Integer, NameNegativeReviewTuple> map = new TreeMap<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int positiveCount = 0;
        NameNegativeReviewTuple tuple = new NameNegativeReviewTuple();
        String[] input = value.toString().split("\\s+");
        int endIndex = value.toString().indexOf(input[input.length-2]);
        String restaurantName = value.toString().substring(0, endIndex);
        tuple.setRestaurantName(restaurantName);
        positiveCount = Integer.parseInt(input[input.length-2]);
        tuple.setNegativeCount(Integer.parseInt(input[input.length-1]));

        if (map.containsKey(positiveCount)) {
            NameNegativeReviewTuple t = map.get(positiveCount);
            if (t.getNegativeCount() > tuple.getNegativeCount()) {
                map.put(positiveCount, tuple);
            }
        } else {
            map.put(positiveCount, tuple);
        }
        if (map.size() > 10) {
            map.remove(map.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Integer, NameNegativeReviewTuple> entry : map.descendingMap().entrySet()) {
            context.write(new Text(entry.getValue().getRestaurantName()), new IntWritable(entry.getKey()));
        }
    }

}
