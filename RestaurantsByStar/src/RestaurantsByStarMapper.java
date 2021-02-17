import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RestaurantsByStarMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private final Pattern csvPattern = Pattern.compile("\"([^\"]*)\"|(?<=,|^)([^,]*)(?:,|$)");
    private final ArrayList<String> allMatches = new ArrayList<>();

    private MultipleOutputs<Text, NullWritable> mMultipleOutputs = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mMultipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (!value.toString().contains("business_id")) {
            String[] input = parse(value.toString());

            if (input[4].equals("5.0")) {
                mMultipleOutputs.write("5", value.toString().replaceAll("\"", ""), NullWritable.get());
            } else if (input[4].equals("4.0") || input[4].equals("4.5")) {
                mMultipleOutputs.write("4", value.toString().replaceAll("\"", ""), NullWritable.get());
            } else if (input[4].equals("3.0") || input[4].equals("3.5")) {
                mMultipleOutputs.write("3", value.toString().replaceAll("\"", ""), NullWritable.get());
            } else if (input[4].equals("2.0") || input[4].equals("2.5")) {
                mMultipleOutputs.write("2", value.toString().replaceAll("\"", ""), NullWritable.get());
            } else {
                mMultipleOutputs.write("1", value.toString().replaceAll("\"", ""), NullWritable.get());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mMultipleOutputs.close();
    }

    public String[] parse(String csvLine)
    {
        Matcher matcher = csvPattern.matcher(csvLine);
        allMatches.clear();
        String match;
        while (matcher.find()) {
            match = matcher.group(1);
            if (match != null) {
                allMatches.add(match);
            } else {
                allMatches.add(matcher.group(2));
            }
        }

        int size = allMatches.size();
        if (size > 0) {
            return allMatches.toArray(new String[size]);
        } else {
            return new String[0];
        }
    }
}
