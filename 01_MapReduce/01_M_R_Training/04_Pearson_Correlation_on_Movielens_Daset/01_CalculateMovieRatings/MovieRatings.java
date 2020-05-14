package PearsonCorrelation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MovieRatings extends Configured implements Tool {

    public static class MovieRatingsMapper extends Mapper<LongWritable, Text, Text, RatingWritable>{
        private static final Text currMovieId = new Text();
        private static final RatingWritable rating = new RatingWritable();

        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            if( offset.get() == 0)
                return;                     // pass the header line of ratings data

            // userId, movieId, rating, timestamp
            String[] ratingsData = line.toString().split(",");

            currMovieId.set(ratingsData[1]);

            // Rating Writable is defined by me.
            // Consists of userId,rating
            rating.setUserId(Integer.parseInt(ratingsData[0]));
            rating.setRating(Double.parseDouble(ratingsData[2]));

            context.write(currMovieId, rating);
        }
    }

    public static class MovieRatingsReducer extends Reducer<Text, RatingWritable, Text, MapWritable> {
        @Override
        protected void reduce(Text key, Iterable<RatingWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable map = new MapWritable();

            for(RatingWritable rating : values)
                map.put(new IntWritable(rating.getUserId()), new DoubleWritable(rating.getRating()));

            context.write(key, map);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        String iPath = "hdfs://master:9000/user/herolenk/ml-latest-small/ratings.csv";
        String oPath = "hdfs://master:9000/user/herolenk/outputs/movieRatings";

        Path inputPath = new Path(iPath);
        Path outputPath = new Path(oPath);

        Configuration configuration = new Configuration();
        configuration.set("mapred.textoutputformat.separator", ",");

        Job job = Job.getInstance(configuration);
        job.setJarByClass(MovieRatings.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(MovieRatingsMapper.class);
        job.setReducerClass(MovieRatingsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RatingWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        // For debugging purposes, overwrite the old output
        outputPath.getFileSystem(configuration).delete(outputPath, true);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int errCode = -1;

        try{
            errCode = ToolRunner.run(new Configuration(), new MovieRatings(), args);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.exit(errCode);
    }
}
