package AvgMovieRating;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringJoiner;

public class MovieRaters extends Configured implements Tool {

    public static class RatingsMapper extends Mapper<LongWritable, Text, Text, Text>{
        private static final Text currMovieId = new Text();
        private static final Text currData = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            if( offset.get() == 0)
                return;                     // pass the header line of ratings data

            // userId, movieId, rating, timestamp
            String[] ratingsData = line.toString().split(",");

            currMovieId.set(ratingsData[1]);
            currData.set(ratingsData[0]);           // set userId as data

            context.write(currMovieId, currData);
        }
    }

    public static class RatingsReducer extends Reducer<Text, Text, Text, Text>{

        private static final Text output = new Text();

        @Override
        protected void reduce(Text movieId, Iterable<Text> userIds, Context context) throws IOException, InterruptedException {
            StringJoiner userList = new StringJoiner(",");

            for(Text userId : userIds)
                userList.add(userId.toString());

            output.set(userList.toString());

            context.write(movieId, output);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        String iPath = "hdfs://master:9000/user/herolenk/ml-latest-small/ratings.csv";
        String oPath = "hdfs://master:9000/user/herolenk/outputs/movieRaters";

        Path inputPath = new Path(iPath);
        Path outputPath = new Path(oPath);

        Configuration configuration = new Configuration();
        configuration.set("mapred.textoutputformat.separator", ",");

        Job job = Job.getInstance(configuration);
        job.setMapperClass(RatingsMapper.class);
        job.setReducerClass(RatingsReducer.class);
        job.setJarByClass(MovieRaters.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // For debugging purposes, overwrite the old output
        outputPath.getFileSystem(configuration).delete(outputPath, true);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int errCode = -1;

        try{
            errCode = ToolRunner.run(new Configuration(), new MovieRaters(), args);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.exit(errCode);
    }
}
