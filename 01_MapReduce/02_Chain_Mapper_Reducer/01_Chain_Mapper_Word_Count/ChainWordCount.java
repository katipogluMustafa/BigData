package Chain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class ChainWordCount extends Configured implements Tool {
    private static final IntWritable ONE = new IntWritable(1);

    public static class SplitMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            Text currWord = new Text();
            for(String word: words){
                currWord.set(word);
                context.write(currWord, ONE);
            }
        }
    }

    public static class UpperCaseMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
        private static final Text word = new Text();

        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String upperCaseWord = key.toString().toUpperCase();
            word.set(upperCaseWord);
            context.write(word, value);
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int counter = StreamSupport.stream(values.spliterator(), true).mapToInt(IntWritable::get).sum();
            context.write(key, new IntWritable(counter));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        String iPath = "hdfs://master:9000/words.txt";
        String oPath = "hdfs://master:9000/user/herolenk/outputs/chainWordCount";

        Path inputPath = new Path(iPath);
        Path outputPath = new Path(oPath);

        Configuration configuration = new Configuration();
        configuration.set("mapred.textoutputformat.separator", ",");

        Job job = Job.getInstance(configuration);

        // ----------------- Start Chain Mapper Special -----------------

        // Add First Mapper Class
        Configuration splitMapConfig = new Configuration(false);
        ChainMapper.addMapper(job, SplitMapper.class, Object.class, Text.class, Text.class, IntWritable.class, splitMapConfig);

        // Add Second Mapper Class
        Configuration upperCaseMapperConfig = new Configuration(false);
        ChainMapper.addMapper(job, UpperCaseMapper.class, Text.class, IntWritable.class, Text.class, IntWritable.class, upperCaseMapperConfig);

        // ----------------- END Chain Mapper Special -----------------

        job.setJarByClass(ChainWordCount.class);

        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // For debugging purposes, overwrite the old output
        outputPath.getFileSystem(configuration).delete(outputPath, true);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int errCode = -1;

        try{
            errCode = ToolRunner.run(new Configuration(), new ChainWordCount(), args);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.exit(errCode);
    }
}
