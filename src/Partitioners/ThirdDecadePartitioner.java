
package Partitioners;

import Model.AmountCw1;
import Model.FinalKey;
import Model.Npmi;
import Model.PairInDecade;
import org.apache.hadoop.mapreduce.Partitioner;

/**
     * Created by RazB on 05/03/2017.
     */

    public class ThirdDecadePartitioner extends Partitioner<PairInDecade, AmountCw1> {
        @Override
        public int getPartition(PairInDecade key, AmountCw1 value, int numPartitions) {
            return key.getDecade() % numPartitions;
        }

}
