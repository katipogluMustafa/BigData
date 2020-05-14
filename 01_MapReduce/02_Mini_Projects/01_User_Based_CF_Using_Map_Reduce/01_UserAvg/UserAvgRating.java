package Project.UserAvg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class UserAvgRating extends Configured implements Tool {

    public static class AvgMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(key.get() == 0)   // if the first line, skip since csv header is found there.
                return;
            // UserId, MovieId, Rating, Timestamp
            String[] inputLine = value.toString().split(",");
            // Key-> UserId   Value -> Rating
            context.write(new IntWritable(Integer.parseInt(inputLine[0])),
                          new DoubleWritable(Double.parseDouble(inputLine[2])));
        }
    }
    public static class AvgReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>{
        @Override
        protected void reduce(IntWritable userId, Iterable<DoubleWritable> ratings, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0;

            for(DoubleWritable rating: ratings){
                sum += rating.get();
                count++;
            }
            // Key -> userId , Value -> Average of the User
            context.write(userId, new DoubleWritable(sum/count));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        String iPath = "hdfs://master:9000/user/herolenk/ml-latest-small/ratings.csv";
        String oPath = "hdfs://master:9000/user/herolenk/outputs/userAvgRating";

        Path inputPath = new Path(iPath);
        Path outputPath = new Path(oPath);

        Configuration configuration = new Configuration();
        configuration.set("mapred.textoutputformat.separator", ",");

        Job job = Job.getInstance(configuration);
        job.setMapperClass(AvgMapper.class);
        job.setReducerClass(AvgReducer.class);
        job.setJarByClass(UserAvgRating.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        // For debugging purposes, overwrite the old output
        outputPath.getFileSystem(configuration).delete(outputPath, true);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int errCode = -1;

        try{
            errCode = ToolRunner.run(new Configuration(), new UserAvgRating(), args);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.exit(errCode);
    }
}
