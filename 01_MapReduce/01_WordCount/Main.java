import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String iPath = "hdfs://master:9000/data";
        String oPath = "hdfs://master:9000/out";

        Path inputPath = new Path(iPath);
        Path outputPath = new Path(oPath);

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setJarByClass(Main.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        outputPath.getFileSystem(configuration).delete(outputPath, true);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
