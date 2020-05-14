package PearsonCorrelation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class PearsonCorrelation extends Configured implements Tool {

     public static class PearsonMapper extends Mapper<Text, MapWritable, IntPairWritable, DoublePairWritable>{

         @Override
         protected void map(Text movieId, MapWritable map, Context context) throws IOException, InterruptedException {

             // Enumerate all items found in the map
             for(Map.Entry<Writable, Writable> entry : map.entrySet()){
                IntWritable raterId = (IntWritable) entry.getKey();
                DoubleWritable rating = (DoubleWritable) entry.getValue();

                 // For each of the item, again enumerate all items found in the map
                 for(Map.Entry<Writable, Writable> entry2 : map.entrySet()) {
                     IntWritable rater2Id = (IntWritable) entry2.getKey();
                     DoubleWritable rating2 = (DoubleWritable) entry2.getValue();

                     if( raterId == rater2Id )
                         continue;

                     IntPairWritable idPair = new IntPairWritable(raterId.get(), rater2Id.get());
                     DoublePairWritable ratingPair = new DoublePairWritable(rating.get(), rating2.get());

                     // Write (userId1, userId2), (userId1Rating, userId2Rating)
                     context.write(idPair, ratingPair);

                 }
            }

         }
     }

     public static class PearsonReducer extends Reducer<IntPairWritable, DoublePairWritable, IntPairWritable, DoubleWritable>{

         public static HashMap<Integer, Double> avgRatings = new HashMap<>();

         @Override
         public void setup(Context context) {
             // Here we store userId,avgRating pairs inside ram using hashmap

             // Why I choose to store them inside ram?
             // lets assume we have 1 billion users
             // each record of map Int,Double so 12byte approximately,
             // total cost of map would be 12gb of ram, which is definitely acceptable at this scale
             // for 1 million users, only 12mb


             Configuration conf = context.getConfiguration();

             //Set in the job runner and retrieve here
             String location = conf.get("job.avgratings.path");

             String[] tokens;
             if (location != null) {
                 BufferedReader br = null;
                 try {
                     FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf);
                     Path path = new Path(location);
                     if (fs.exists(path)) {
                         FSDataInputStream fis = fs.open(path);
                         br = new BufferedReader(new InputStreamReader(fis));
                         String line = null;
                         while ((line = br.readLine()) != null && line.trim().length() > 0) {
                             tokens = line.split(",");
                             avgRatings.put(Integer.parseInt(tokens[0]), Double.parseDouble(tokens[1]));
                         }
                         fis.close();
                     }
                 }
                 catch (Exception e) {
                     e.printStackTrace();
                     System.exit(-2);
                 }
                 finally {
                     IOUtils.closeStream(br);
                 }
             }
         }

         @Override
         protected void reduce(IntPairWritable userPair, Iterable<DoublePairWritable> ratings, Context context) throws IOException, InterruptedException {
             // Use User Based Pearson Correlation Formula
             // Here reduce method takes inputs of {(user1Id,user2Id), (rating1,rating2)}
             // These data are only generated when both user1 and user2 given rating to the same movie

             int userId1 = userPair.getFirst();
             int userId2 = userPair.getSecond();

             double userId1Avg = avgRatings.get(userId1);
             double userId2Avg = avgRatings.get(userId2);

             double rating1, rating2;                      // userId1, rating1  - userId2, rating2
             double temp1, temp2;

             double sum = 0;
             double denominatorSum1 = 0;
             double denominatorSum2 = 0;
             for(DoublePairWritable rating : ratings){
                 rating1 = rating.getFirst();
                 rating2 = rating.getSecond();
                 temp1 = (rating1 - userId1Avg);
                 temp2 = (rating2 - userId2Avg);
                 sum += temp1 * temp2;
                 denominatorSum1 += Math.pow(temp1, 2);
                 denominatorSum2 += Math.pow(temp2, 2);
             }

             double pearsonCorr = sum / (Math.sqrt(denominatorSum1) * Math.sqrt(denominatorSum2));

             context.write(userPair, new DoubleWritable(pearsonCorr));
         }
     }

    @Override
    public int run(String[] args) throws Exception {
         // movieRatings calculated before in the form of SequenceFileOutputFormat
        String iPath = "hdfs://master:9000/user/herolenk/outputs/movieRatings";
        String oPath = "hdfs://master:9000/user/herolenk/outputs/pearsonCorr";

        String avgRatings = "hdfs://master:9000/user/herolenk/outputs/userAvgRating/part-r-00000";

        Path inputPath = new Path(iPath);
        Path outputPath = new Path(oPath);

        Configuration configuration = new Configuration();
        configuration.set("mapred.textoutputformat.separator", ",");
        configuration.set("job.avgratings.path", avgRatings);

        Job job = Job.getInstance(configuration);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(PearsonMapper.class);
        job.setReducerClass(PearsonReducer.class);
        job.setJarByClass(PearsonCorrelation.class);

        job.setMapOutputKeyClass(IntPairWritable.class);
        job.setMapOutputValueClass(DoublePairWritable.class);

        job.setOutputKeyClass(IntPairWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        // For debugging purposes, overwrite the old output
        outputPath.getFileSystem(configuration).delete(outputPath, true);

        KeyValueTextInputFormat.addInputPath(job, inputPath);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int errCode = -1;

        try{
            errCode = ToolRunner.run(new Configuration(), new PearsonCorrelation(), args);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.exit(errCode);
    }
}
