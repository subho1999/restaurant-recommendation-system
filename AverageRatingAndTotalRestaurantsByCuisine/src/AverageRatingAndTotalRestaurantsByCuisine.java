import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AverageRatingAndTotalRestaurantsByCuisine {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 2) {
            System.err.println("Usage: hadoop jar RestaurantByCuisine.jar <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Total Restaurants by Cuisine and Their Average Rating");

        job.setJarByClass(AverageRatingAndTotalRestaurantsByCuisine.class);
        job.setMapperClass(AverageRatingAndTotalRestaurantsByCuisineMapper.class);
        job.setReducerClass(AverageRatingAndTotalRestaurantsByCuisineReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RatingCountTuple.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
