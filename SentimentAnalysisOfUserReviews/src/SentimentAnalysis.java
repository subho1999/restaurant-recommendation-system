import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class SentimentAnalysis {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        if (args.length != 4) {
            System.err.println("Usage: hadoop jar SentimentAnalysis.jar <input path> <output path> " +
                    "<positive lexicon file path> <negative lexicon file path>");
            System.exit(-1);
        }

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Sentiment Analysis of User Reviews");

        job.addCacheFile(new URI(args[2]));
        job.addCacheFile(new URI(args[3]));

        //job.setJarByClass(SentimentAnalysis.class);
        job.setMapperClass(SentimentAnalysisMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
