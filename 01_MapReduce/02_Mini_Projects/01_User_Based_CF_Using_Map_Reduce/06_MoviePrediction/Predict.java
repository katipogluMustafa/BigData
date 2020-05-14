package Project.MoviePrediction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Predict extends Configured implements Tool {

    public static int userId = 449;
    public static int movieId = 32;

    public static class KNNFilterMapper extends Mapper<IntWritable, MapWritable, Text, MapWritable> {
        // Input Format-> userId1, Map of userId2,corr pairs

        /**
         * Filter only KNN date for the userId
         */
        @Override
        protected void map(IntWritable key, MapWritable value, Context context) throws IOException, InterruptedException {
            if(key.get() != Predict.userId)
                return;

            // In order to differentiate Mapper outputs, put + in front of this filter's outputs
            context.write(new Text("k" + key.toString()), value);
        }
    }

    /**
     * Filter only movie data for movieId
     */
    public static class MovieRatingsFilterMapper extends Mapper<Text, MapWritable, Text, MapWritable>{
        @Override
        protected void map(Text key, MapWritable value, Context context) throws IOException, InterruptedException {
            if(Integer.parseInt(key.toString()) != Predict.movieId)
                return;

            // In order to differentiate Mapper outputs, put - in front of this filter's outputs
            key.set("m" + key.toString());

            context.write(key, value);
        }
    }

    public static class PredictReducer extends Reducer<Text, MapWritable, Text, Text>{
        public Map<Integer, Double> movieRatings = new HashMap<Integer, Double>();
        public Map<Integer, Double> knn = new HashMap<Integer, Double>();

        @Override
        protected void reduce(Text key, Iterable<MapWritable> maps, Context context) throws IOException, InterruptedException {
            // Get The movieRatings and knn into static variables.
            String data = key.toString();
            if( data.startsWith("m") )
                for(MapWritable map : maps)
                    for(Map.Entry<Writable, Writable> entry: map.entrySet())
                        movieRatings.put(((IntWritable)(entry.getKey())).get(),
                                        ((DoubleWritable)(entry.getValue())).get());

            if(data.startsWith("k"))
                for(MapWritable map: maps)
                    for(Map.Entry<Writable, Writable> entry: map.entrySet())
                        knn.put(((IntWritable)(entry.getKey())).get(),
                                ((DoubleWritable)(entry.getValue())).get());

            if( movieRatings.isEmpty() || knn.isEmpty())
                return;

            // Computer Prediction
            double weightedSum = 0;
            double weightSum = 0;
            for(Map.Entry<Integer, Double> entry : knn.entrySet()){
                Integer neighbourId = entry.getKey();
                Double correlation = entry.getValue();
                Double neighbourRating = movieRatings.getOrDefault(neighbourId,-1.0);
                if(neighbourRating == -1.0)
                    continue;
                //System.out.println("Rating: " + neighbourRating + "NeighbourId: " + neighbourId +"Corr: " + correlation);
                weightedSum += neighbourRating * correlation;
                weightSum += correlation;
            }
            double prediction = weightedSum / weightSum;
            context.write(new Text(Integer.toString(userId)), new Text(Double.toString(prediction)));
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        // Input Files
        String movieRatings = "hdfs://master:9000/user/herolenk/outputs/movieRatings/";
        String knn = "hdfs://master:9000/user/herolenk/outputs/knn2/";

        // Output Files
        String movieRec = "hdfs://master:9000/user/herolenk/outputs/movieRec";

        // Lets get paths as Path object
        Path movieRatingsPath = new Path(movieRatings);
        Path knnPath = new Path(knn);
        Path movieRecPath = new Path(movieRec);

        // Lets configure the join job
        Configuration configuration = new Configuration();

        // Set the output delimiter as comma instead of default \t
        configuration.set("mapred.textoutputformat.separator", ",");

        Job job = Job.getInstance(configuration);
        job.setJarByClass(Predict.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        // Set the Reducer
        job.setReducerClass(PredictReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, movieRecPath);

        // Set the Mappers for related paths
        MultipleInputs.addInputPath(job, movieRatingsPath, SequenceFileInputFormat.class, MovieRatingsFilterMapper.class);
        MultipleInputs.addInputPath(job, knnPath, SequenceFileInputFormat.class, KNNFilterMapper.class);

        // For debugging purposes, overwrite the old output
        movieRecPath.getFileSystem(configuration).delete(movieRecPath, true);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int errCode = -1;

        try{
            errCode = ToolRunner.run(new Configuration(), new Predict(), args);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.exit(errCode);
    }
}
