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
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ZoneTotalSale extends Configured implements Tool {
    private static final IntWritable ONE = new IntWritable(1);

    public static class SplitMapper extends Mapper<Object, Text, Text, Text> {
        private static final Text item = new Text();
        private static final Text data = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Line Format -> item sale zone
            // Example     -> Item1 345 zone-1

            // Split records
            String[] salesArr = value.toString().split(" ");
            item.set(salesArr[0]);

            // Writing (sales,zone) as value
            data.set(salesArr[1] + "," + salesArr[2]);
            context.write(item, data);
        }
    }

    public static class FilterMapper extends Mapper<Text, Text, Text, IntWritable>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");

            // Filter only data related to zone-1
            if(data[1].equals("zone-1"))
                context.write(key, new IntWritable(Integer.parseInt(data[0])));

        }
    }

    public static class TotalSalesReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();

            context.write(key, new IntWritable(sum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        String iPath = "hdfs://master:9000/sales.txt";
        String oPath = "hdfs://master:9000/user/herolenk/outputs/zoneTotalSale";

        Path inputPath = new Path(iPath);
        Path outputPath = new Path(oPath);

        Configuration configuration = new Configuration();
        configuration.set("mapred.textoutputformat.separator", ",");

        Job job = Job.getInstance(configuration);

        // ----------------- Start Chain Mapper Special -----------------

        // Add First Mapper Class
        Configuration splitMapConfig = new Configuration(false);
        ChainMapper.addMapper(job, SplitMapper.class, Object.class, Text.class, Text.class, Text.class, splitMapConfig);

        // Add Second Mapper Class
        Configuration FilterMapConfig = new Configuration(false);
        ChainMapper.addMapper(job, FilterMapper.class, Text.class, Text.class, Text.class, IntWritable.class, FilterMapConfig);

        // ----------------- END Chain Mapper Special -----------------



        // ----------------- Start Chain Reducer Special -----------------
        // Using the predefined ChainReducer class in Hadoop you can chain multiple Mapper classes after a Reducer within the Reducer task.
        // For each record output by the Reducer, the Mapper classes are invoked in a chained fashion.
        // Special care has to be taken when creating chains that the key/values output by a Mapper are valid for the following Mapper in the chain.
        // Reference: https://www.netjstech.com/2018/07/chaining-mapreduce-job-in-hadoop.html

        Configuration reduceConf = new Configuration(false);
        ChainReducer.setReducer(job, TotalSalesReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, reduceConf);

        // Here instead of writing our mapper, we use predefined mappers from hadoop lib
        // InverseMapper, inverses the key values
        ChainReducer.addMapper(job, InverseMapper.class, Text.class, IntWritable.class, IntWritable.class, Text.class, null);

        // ----------------- END Chain Reducer Special -----------------

        job.setJarByClass(ZoneTotalSale.class);

        job.setOutputKeyClass(IntWritable.class);
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
            errCode = ToolRunner.run(new Configuration(), new ZoneTotalSale(), args);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.exit(errCode);
    }
}
