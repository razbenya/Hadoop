package Partitioners;

import Model.FinalKey;
import Model.FinalValue;
import Model.Npmi;
import Model.PairInDecade;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by RazB on 05/03/2017.
 */
    public class ForthDecadePartitioner extends Partitioner<FinalKey, FinalValue> {
        @Override
        public int getPartition(FinalKey key, FinalValue value, int numPartitions) {
            return key.getDecade() % numPartitions;
        }
    }

