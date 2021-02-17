import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TotalAndAverageRatingOfRestaurantsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private final Pattern csvPattern = Pattern.compile("\"([^\"]*)\"|(?<=,|^)([^,]*)(?:,|$)");
    private final ArrayList<String> allMatches = new ArrayList<String>();

    private DoubleWritable rating = new DoubleWritable();
    int count = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (!value.toString().contains("business_id")) {
            String[] input = parse(value.toString());

            // System.out.println(count++);
            rating = new DoubleWritable(Double.parseDouble(input[4]));
            context.write(new Text("Restaurant"), rating);
        }
    }

    public String[] parse(String csvLine) {
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
