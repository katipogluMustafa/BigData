package Project.KNN;

import MovieRecommendation.SecondarySorting.UserPairPearsonCorr;
import MovieRecommendation.SecondarySorting.UserPairPearsonCorrGroupingComparator;
import MovieRecommendation.SecondarySorting.UserPairPearsonCorrPartitioner;
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

public class KNearestNeighbour extends Configured implements Tool {

    public static int k = 20;

    public static class SplitMapper extends Mapper<LongWritable, Text, UserPairPearsonCorr, UserPairPearsonCorr>{
        public static UserPairPearsonCorr pair = new UserPairPearsonCorr();

        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            // Line Format -> userId1, userId2, pearsonCorrelation
            String[] tokens = line.toString().split(",");
            pair.setUserId1(Integer.parseInt(tokens[0]));
            pair.setUserId2(Integer.parseInt(tokens[1]));
            double corr = Double.parseDouble(tokens[2]);
            pair.setCorr(corr);

            // See, both pair contains 'corr' and also our value is the 'corr'[For Secondary Sorting Purposes]
            context.write(pair, pair);
        }
    }

    public static class KNNReducer extends Reducer<UserPairPearsonCorr, UserPairPearsonCorr, IntWritable, MapWritable>{
        @Override
        protected void reduce(UserPairPearsonCorr pairs, Iterable<UserPairPearsonCorr> corrs, Context context) throws IOException, InterruptedException {
            int counter = KNearestNeighbour.k;
            IntWritable key = new IntWritable(pairs.getUserId1());
            MapWritable map = new MapWritable();

            for(UserPairPearsonCorr corr: corrs){
                // Dont process Nan Data
                if(Double.isNaN(corr.getCorr()))
                   continue;

                map.put(new IntWritable(corr.getUserId2()), new DoubleWritable(corr.getCorr()));

                // Only Process K number of data
                counter--;
                if(counter == 0)
                    break;
            }
            context.write(key, map);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        String iPath = "hdfs://master:9000/user/herolenk/outputs/pearsonCorr/part-r-00000";
        String oPath = "hdfs://master:9000/user/herolenk/outputs/knn2";

        Path inputPath = new Path(iPath);
        Path outputPath = new Path(oPath);

        Configuration configuration = new Configuration();
        configuration.set("mapred.textoutputformat.separator", ",");

        Job job = Job.getInstance(configuration);

        job.setJarByClass(KNearestNeighbour.class);
        job.setPartitionerClass(UserPairPearsonCorrPartitioner.class);               // my own partitioner
        job.setGroupingComparatorClass(UserPairPearsonCorrGroupingComparator.class); // my own group comparator

        job.setMapperClass(SplitMapper.class);
        job.setReducerClass(KNNReducer.class);

        job.setMapOutputKeyClass(UserPairPearsonCorr.class);
        job.setMapOutputValueClass(UserPairPearsonCorr.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MapWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // For debugging purposes, overwrite the old output
        outputPath.getFileSystem(configuration).delete(outputPath, true);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int errCode = -1;

        try{
            errCode = ToolRunner.run(new Configuration(), new KNearestNeighbour(), args);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.exit(errCode);
    }
}
