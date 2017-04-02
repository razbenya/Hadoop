package Partitioners;

import Model.Amount;
import Model.PairInDecade;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by RazB on 05/03/2017.
 */
    public class DecadePartitioner extends Partitioner<PairInDecade, Amount> {
        @Override
        public int getPartition(PairInDecade key, Amount value, int numPartitions) {
            return key.getDecade() % numPartitions;
        }
    }

