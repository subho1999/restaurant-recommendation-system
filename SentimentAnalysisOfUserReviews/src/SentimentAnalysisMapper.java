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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SentimentAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Pattern csvPattern = Pattern.compile("(?:^|,)\\s*(?:(?:(?=\")\"([^\"].*?)\")|(?:(?!\")(.*?)))(?=,|$)");
    private final ArrayList<String> allMatches = new ArrayList<>();

    private Text reviewId = new Text();
    private Set<String> positiveWords = new HashSet<>();
    private Set<String> negativeWords = new HashSet<>();

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
        if (!value.toString().contains("business_id")) {
            int posCount = 0;
            int negCount = 0;

            String[] input = parse(value.toString());
            if (input.length == 9) {
                reviewId.set(input[1]);
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
                    context.write(reviewId, new Text("Positive Review"));
                } else {
                    context.write(reviewId, new Text("Negative Review"));
                }
            }
        }
    }

    private void readFile(File file, int count) throws IOException {
        BufferedReader bf = new BufferedReader(new FileReader(file.toString()));
        String word = null;
        while ((word = bf.readLine()) != null) {
            if (count == 0) {
                positiveWords.add(word);
            } else {
                negativeWords.add(word);
            }
        }
        bf.close();
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
