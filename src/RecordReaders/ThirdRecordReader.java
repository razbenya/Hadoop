package RecordReaders;

import Model.AmountCw1;
import Model.PairInDecade;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import java.io.IOException;


/**
 * Created by snoop_000 on 01/03/2017.
 */
public class ThirdRecordReader extends RecordReader<PairInDecade,AmountCw1> {
    private LineRecordReader reader;

    public ThirdRecordReader() {
        reader = new LineRecordReader();
    }
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        reader.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return reader.nextKeyValue();
    }

    @Override
    public PairInDecade getCurrentKey() throws IOException, InterruptedException {
        return new PairInDecade(reader.getCurrentValue().toString());
    }

    @Override
    public AmountCw1 getCurrentValue() throws IOException, InterruptedException {
        return new AmountCw1(reader.getCurrentValue().toString());
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }


}
