import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AverageRatingAndTotalRestaurantsByCuisineMapper extends Mapper<LongWritable, Text, Text, RatingCountTuple> {

    private final Pattern csvPattern = Pattern.compile("\"([^\"]*)\"|(?<=,|^)([^,]*)(?:,|$)");
    private final ArrayList<String> allMatches = new ArrayList<>();

    private RatingCountTuple mTuple = new RatingCountTuple();

    private String[] cuisines = {"pizza", "burger", "american", "mexican", "indian", "chinese", "japanese", "italian", "korean", "sushi"};

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (!value.toString().contains("business_id")) {
            mTuple.setCount(1);
            String[] input = parse(value.toString());
            System.out.println(input[3] + " " + input[4] + " " + input[5]);
            mTuple.setRating(Double.parseDouble(input[4]));
            String str = input[7].replace("\"", "");
            String[] category = str.toString().split(" ");
            Set<String> set = new HashSet<String>();

            for (String cat : category) {
                set.add(cat.toLowerCase());
            }
            for (String cuisine : cuisines) {
                if (set.contains(cuisine)) {
                    context.write(new Text(cuisine), mTuple);
                }
            }
        }
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
