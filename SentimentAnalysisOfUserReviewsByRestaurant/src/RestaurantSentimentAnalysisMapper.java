import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RestaurantSentimentAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Pattern csvPattern = Pattern.compile("(?:^|,)\\s*(?:(?:(?=\")\"([^\"].*?)\")|(?:(?!\")(.*?)))(?=,|$)");
    private final ArrayList<String> allMatches = new ArrayList<>();

    private Text restaurantId = new Text();
    private Text restaurantName = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (!value.toString().contains("business_id")) {
            String[] input = parse(value.toString());
            restaurantId.set(input[0].replaceAll("\"", ""));
            restaurantName.set("A" + " " + input[1]);
            context.write(restaurantId, restaurantName);
        }
    }

    public String[] parse(String csvLine) {
        return getStrings(csvLine, csvPattern, allMatches);
    }

    static String[] getStrings(String csvLine, Pattern csvPattern, ArrayList<String> allMatches) {
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
