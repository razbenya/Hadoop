package InputFormats;

import Model.AmountCw1;
import Model.PairInDecade;
import RecordReaders.ThirdRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

/**
 * Created by snoop_000 on 01/03/2017.
 */
public class ThirdInputFormat extends SequenceFileInputFormat<PairInDecade, AmountCw1> {
    @Override
    public RecordReader<PairInDecade,AmountCw1> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new ThirdRecordReader();
    }
}
