package Main;

import InputFormats.ForthInputFormat;
import InputFormats.ThirdInputFormat;
import MappersAndReducers.ForthMapperAndReducer;
import Model.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by Neta on 2017-03-08.
 */
public class StepFour extends Configured implements Tool {
    private static String outputPath;
    private static final String inputPath = "s3n://razbucket2/TempOutPut3";

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new StepFour(), args);
    }

    private static void runJob4(Configuration jobconf4) throws IOException, ClassNotFoundException, InterruptedException {
        Job job4 = new Job(jobconf4);
        job4.setJarByClass(ForthMapperAndReducer.class);
        job4.setMapperClass(ForthMapperAndReducer.MapperClass.class);
        job4.setPartitionerClass(Partitioners.ForthDecadePartitioner.class);
        job4.setReducerClass(ForthMapperAndReducer.Reduce.class);
        job4.setMapOutputKeyClass(FinalKey.class);
        job4.setMapOutputValueClass(FinalValue.class);
        job4.setOutputKeyClass(PairInDecade.class);
        job4.setOutputValueClass(Npmi.class);
        job4.setInputFormatClass(ForthInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        ThirdInputFormat.addInputPath(job4, new Path(inputPath));
        TextOutputFormat.setOutputPath(job4, new Path(outputPath));
        System.out.println("waitin for complition of job4");
        job4.waitForCompletion(true);
        System.out.println("deleting files of job3");
        //FileSystem hdfs = FileSystem.get(jobconf4);
        //hdfs.delete(new Path(path+"3"),true);
    }


    public int run(String[] args) throws Exception {
        try {
            Configuration jobconf = new Configuration();
            if(args.length == 4) {
                args[0] = args[1];
                args[1] = args[2];
                args[2] = args[3];
            }
            outputPath = args[0];
            jobconf.set("minPmi", args[1]);
            jobconf.set("relMinPmi", args[2]);

            System.out.println("running job 1");
            runJob4(jobconf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 1;
    }
}
