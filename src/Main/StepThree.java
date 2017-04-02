package Main;

import InputFormats.SecondInputFormat;
import InputFormats.ThirdInputFormat;
import MappersAndReducers.FirstMapperAndReducer;
import MappersAndReducers.SecondMapperAndReducer;
import MappersAndReducers.ThirdMapperAndReducer;
import Model.Amount;
import Model.AmountCw1;
import Model.Npmi;
import Model.PairInDecade;
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
public class StepThree extends Configured implements Tool {
    private static final String inputPath = "s3n://razbucket2/TempOutPut2";
    private static final String outputPath = "s3n://razbucket2/TempOutPut3";

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new StepThree(), args);
    }

    private static void runJob3(Configuration jobconf3) throws IOException, ClassNotFoundException, InterruptedException {
        Job job3 = new Job(jobconf3);
        job3.setJarByClass(ThirdMapperAndReducer.class);

        job3.setMapperClass(ThirdMapperAndReducer.MapperClass.class);
        job3.setReducerClass(ThirdMapperAndReducer.Reduce.class);
        job3.setPartitionerClass(Partitioners.ThirdDecadePartitioner.class);
        job3.setMapOutputKeyClass(PairInDecade.class);
        job3.setMapOutputValueClass(AmountCw1.class);
        job3.setOutputKeyClass(PairInDecade.class);
        job3.setOutputValueClass(Npmi.class);

        job3.setInputFormatClass(ThirdInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        ThirdInputFormat.addInputPath(job3, new Path(inputPath));
        TextOutputFormat.setOutputPath(job3, new Path(outputPath));

        System.out.println("waitin for complition of job3");

        job3.waitForCompletion(true);
        System.out.println("deleting files of job2");
        //   FileSystem hdfs = FileSystem.get(jobconf3);
        // hdfs.delete(new Path(path+"2"),true);
    }

    public int run(String[] args) throws Exception {
        try {
            Configuration jobconf = new Configuration();
            System.out.println("running job 3");
            runJob3(jobconf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 1;
    }
}
