import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class RestaurantSentimentAnalysis {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        if (args.length != 5) {
            System.err.println("Usage: hadoop jar Sentiment.jar <business input path> <review input path> " +
                    "<output path> <positive words path> <negative words path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Reviews of Restaurants based on Sentiment Analysis");

        job.setJarByClass(RestaurantSentimentAnalysis.class);
        job.addCacheFile(new URI(args[3]));
        job.addCacheFile(new URI(args[4]));

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RestaurantSentimentAnalysisMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ReviewMapper.class);

        job.setReducerClass(RestaurantSentimentAnalysisReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
