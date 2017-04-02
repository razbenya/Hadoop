package InputFormats;

import Model.Amount;
import Model.PairInDecade;
import RecordReaders.SecondRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

/**
 * Created by snoop_000 on 28/02/2017.
 */
public class SecondInputFormat extends SequenceFileInputFormat<PairInDecade, Amount> {

    @Override
    public RecordReader<PairInDecade,Amount> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new SecondRecordReader();
    }
}
