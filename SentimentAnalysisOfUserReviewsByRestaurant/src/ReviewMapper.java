import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class ReviewMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Pattern csvPattern = Pattern.compile("(?:^|,)\\s*(?:(?:(?=\")\"([^\"].*?)\")|(?:(?!\")(.*?)))(?=,|$)");
    private final ArrayList<String> allMatches = new ArrayList<>();

    private final Text restaurantId = new Text();
    private final Set<String> positiveWords = new HashSet<>();
    private final Set<String> negativeWords = new HashSet<>();

    @SuppressWarnings("deprecation")
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] lexiconFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        File posFile = new File(lexiconFiles[0].getName());
        File negFile = new File(lexiconFiles[1].getName());
        readFile(posFile, 0);
        readFile(negFile, 1);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (!value.toString().contains("review_id")) {
            int posCount = 0;
            int negCount = 0;

            String[] input = parse(value.toString());
            if (input.length == 9) {
                restaurantId.set(input[0].toString().replaceAll("\"", ""));
                String review = input[7].toLowerCase();
                String[] reviewTokens = review.split(" ");
                for (String token : reviewTokens) {
                    if (positiveWords.contains(token)) {
                        posCount++;
                    } else if (negativeWords.contains(token)) {
                        negCount++;
                    }
                }
                if (posCount >= negCount) {
                    context.write(restaurantId, new Text("Positive Review"));
                } else {
                    context.write(restaurantId, new Text("Negative Review"));
                }
            }
        }
    }

    private void readFile(File file, int count) throws IOException {
        BufferedReader bf = new BufferedReader(new FileReader(file.toString()));
        String word;
        while ((word = bf.readLine()) != null) {
            if (count == 0) {
                positiveWords.add(word);
            } else {
                negativeWords.add(word);
            }
        }
        bf.close();
    }

    public String[] parse(String csvLine) {
        return RestaurantSentimentAnalysisMapper.getStrings(csvLine, csvPattern, allMatches);
    }

}
