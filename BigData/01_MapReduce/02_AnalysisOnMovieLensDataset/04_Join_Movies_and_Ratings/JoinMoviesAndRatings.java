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

public class JoinMoviesAndRatings extends Configured implements Tool {

    public static class MoviesParser extends Mapper<LongWritable, Text, Text, Text> {
        // We will be using MovieId as the join attribute
        private static final Text currMovieId = new Text();

        // In this case, I only care title in movie data other than join attr
        private static final Text currTitle = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            if( offset.get() == 0)
                return;                         // pass the header line of movies data

            // movieId, title, genres(Pipe Separated)
            String[] movieData = line.toString().split(",");

            currMovieId.set(movieData[0]);

            // Add table number to the first index
            // so that we can differentiate the data at the reducer
            currTitle.set("1" + movieData[1]);

            // Key -> movieId   Value -> title
            context.write(currMovieId, currTitle);
        }
    }

    public static class RatingsParser extends Mapper<LongWritable, Text, Text, Text>{
        // MovieId is the join attribute
        private static final Text currMovieId = new Text();

        // Here we care userId, rating adn timestamp as well, so get them all into currData
        private static final Text currData = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            if( offset.get() == 0)
                return;                     // pass the header line of ratings data

            // userId, movieId, rating, timestamp
            String[] ratingsData = line.toString().split(",");

            currMovieId.set(ratingsData[1]);

            // Collect all other attributes in the currData
            StringBuilder dataCombiner = new StringBuilder();
            dataCombiner.append("\t");               // To differentiate between different rating data
            dataCombiner.append(ratingsData[0]);    // Append userId, we'll need userId
            dataCombiner.append(",");
            dataCombiner.append(ratingsData[2]);    // Append rating, we'll surely need rating
            dataCombiner.append(",");
            dataCombiner.append(ratingsData[3]);    // Append timestamp, we may need it, no harm to add :P

            // Add table number to the first index
            // so that we can differentiate the data at the reducer
            currData.set("2" + dataCombiner.toString());

            context.write(currMovieId, currData);
        }
    }

    public static class Joiner extends Reducer<Text, Text, Text, Text>{

        private static final Text output = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder table1 = new StringBuilder();
            StringBuilder table2 = new StringBuilder();

            // Join all attributes
            for(Text text: values){
                String str = text.toString();
                if(str.substring(0,1).equals("1"))
                    table1.append(str.substring(1));
                else
                    table2.append(str.substring(1));

            }

            // Save the output as Text
            output.set(table1.toString() + table2.toString());

            context.write(key, output);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // Input Files
        String ratingsPath = "hdfs://master:9000/user/herolenk/ml-latest-small/ratings.csv";
        String moviesPath = "hdfs://master:9000/user/herolenk/ml-latest-small/movies.csv";
        // Output Files
        String movieRatingsPath = "hdfs://master:9000/user/herolenk/outputs/movieRatings";

        // Lets get paths as Path object
        Path ratings = new Path(ratingsPath);
        Path movies = new Path(moviesPath);
        Path movieRatings = new Path(movieRatingsPath);

        // Lets configure the join job
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(JoinMoviesAndRatings.class);

        // Set the Mappers for related paths
        MultipleInputs.addInputPath(job, ratings, TextInputFormat.class, RatingsParser.class);
        MultipleInputs.addInputPath(job, movies, TextInputFormat.class, MoviesParser.class);

        // Set the Reducer
        job.setReducerClass(Joiner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, movieRatings);

        // For debugging purposes, overwrite the old output
        movieRatings.getFileSystem(configuration).delete(movieRatings, true);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int errCode = -1;

        try{
            errCode = ToolRunner.run(new Configuration(), new JoinMoviesAndRatings(), args);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.exit(errCode);
    }
}
