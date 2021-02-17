import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.xml.soap.Text;
import java.io.IOException;

public class RestaurantsByStar {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 2) {
            System.err.println("Usage: hadoop jar RestaurantByCuisine.jar <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Restaurants By Star using Binning");

        job.setJarByClass(RestaurantsByStar.class);
        job.setMapperClass(RestaurantsByStarMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        MultipleOutputs.addNamedOutput(job, "5", TextOutputFormat.class, Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "4", TextOutputFormat.class, Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "3", TextOutputFormat.class, Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "2", TextOutputFormat.class, Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "1", TextOutputFormat.class, Text.class, NullWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
