import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EliteUsersReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (key.get() > 10000) {
            for (Text userId : values) {
                context.write(userId, key);
            }
        }
    }
}
