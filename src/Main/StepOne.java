package Main;

import MappersAndReducers.FirstMapperAndReducer;
import Model.Amount;
import Model.PairInDecade;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
  * Created by Razb on 2017-03-08.
  */
public class StepOne extends Configured implements Tool {
    private static String inputPath;
    private static final String outputPath = "s3n://razbucket2/TempOutPut";

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new StepOne(), args);
    }

    public int run(String[] args) throws Exception {
        if(args.length == 4) {
            args[0] = args[1];
            args[1] = args[2];
            args[2] = args[3];
        }
        inputPath = args[0];
        try {
            Configuration jobconf1 = new Configuration();
            jobconf1.set("language", args[1]);
            jobconf1.set("stopWords", args[2]);
            Job job1 = new Job(jobconf1, "CollocationExtraction");
            job1.setJarByClass(FirstMapperAndReducer.class);

            job1.setOutputKeyClass(PairInDecade.class);
            job1.setOutputValueClass(Amount.class);

            job1.setMapperClass(FirstMapperAndReducer.MapperClass.class);
            job1.setPartitionerClass(Partitioners.DecadePartitioner.class);
            job1.setReducerClass(FirstMapperAndReducer.Reduce.class);
            job1.setCombinerClass(FirstMapperAndReducer.Reduce.class);

            job1.setInputFormatClass(SequenceFileInputFormat.class);
            job1.setOutputFormatClass(TextOutputFormat.class);

            SequenceFileInputFormat.addInputPath(job1, new Path(inputPath));
            TextOutputFormat.setOutputPath(job1, new Path(outputPath));
            System.out.println("waitin for complition of job1");
            job1.waitForCompletion(true);
            
            System.out.println("running job 1");

        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
        return 1;
    }
}
