package InputFormats;

import Model.Npmi;
import Model.PairInDecade;
import RecordReaders.ForthRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

/**
 * Created by snoop_000 on 01/03/2017.
 */
public class ForthInputFormat extends SequenceFileInputFormat<PairInDecade, Npmi> {
    @Override
    public RecordReader<PairInDecade,Npmi> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new ForthRecordReader();
    }
}
