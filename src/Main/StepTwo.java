package Main;

import InputFormats.SecondInputFormat;
import MappersAndReducers.FirstMapperAndReducer;
import MappersAndReducers.SecondMapperAndReducer;
import Model.Amount;
import Model.AmountCw1;
import Model.PairInDecade;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by Neta on 2017-03-08.
 */
public class StepTwo extends Configured implements Tool {
    private static final String inputPath="s3n://razbucket2/TempOutPut";
    private static final String outputPath = "s3n://razbucket2/TempOutPut2";

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new StepTwo(), args);
    }

    private static void runJob2(Configuration jobconf2) throws IOException, ClassNotFoundException, InterruptedException {

        Job job2 = new Job(jobconf2);
        job2.setJarByClass(SecondMapperAndReducer.class);

        job2.setMapperClass(SecondMapperAndReducer.MapperClass.class);
        job2.setPartitionerClass(Partitioners.DecadePartitioner.class);
        job2.setReducerClass(SecondMapperAndReducer.Reduce.class);
        job2.setMapOutputKeyClass(PairInDecade.class);
        job2.setMapOutputValueClass(Amount.class);
        job2.setOutputKeyClass(PairInDecade.class);
        job2.setOutputValueClass(AmountCw1.class);

        job2.setInputFormatClass(SecondInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        SecondInputFormat.addInputPath(job2, new Path(inputPath));
        TextOutputFormat.setOutputPath(job2, new Path(outputPath));
        System.out.println("waiting for job2 to complete");
        job2.waitForCompletion(true);
        //System.out.println("deleting files of job1");
        //FileSystem hdfs = FileSystem.get(jobconf2);
        //hdfs.delete(new Path(path),true);
    }

    public int run(String[] args) throws Exception {
        try {
            Configuration jobconf = new Configuration();
            System.out.println("running job 2");
            runJob2(jobconf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 1;
    }
}
