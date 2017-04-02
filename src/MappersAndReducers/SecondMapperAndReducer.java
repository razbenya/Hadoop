package MappersAndReducers;

import Model.Amount;
import Model.AmountCw1;
import Model.FinalValue;
import Model.PairInDecade;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by snoop_000 on 28/02/2017.
 */
public class SecondMapperAndReducer {

    public static class MapperClass extends Mapper<PairInDecade, Amount, PairInDecade,Amount> {
        @Override
        public void map(PairInDecade pairInDecade, Amount amount, Context context) throws IOException,  InterruptedException {
            context.write(new PairInDecade(pairInDecade.getWord1(), "*", pairInDecade.getDecade()),amount);
            context.write(pairInDecade, amount);
        }
    }

    public static class Reduce extends Reducer<PairInDecade, Amount, PairInDecade, AmountCw1> {

        private int cw1;
        private int currentDecade;
        private String currentW1;


        protected void setup(Context context) throws IOException, InterruptedException {
            cw1 = 0;
            currentDecade = 0;
            currentW1 = "";
        }

        public void reduce(PairInDecade pairInDecade, Iterable<Amount> values, Context context) throws IOException, InterruptedException {
            if(!currentW1.equals(pairInDecade.getWord1())){
                currentW1 = pairInDecade.getWord1();
                cw1 = 0;
            }
            if(currentDecade != pairInDecade.getDecade()){
                cw1 = 0;
                currentDecade = pairInDecade.getDecade();
            }
            if(pairInDecade.getWord2().equals("*")){
                for (Amount amount : values) {
                    cw1 += amount.getAmount();
                }
            } else {
                for (Amount amount : values) {
                    context.write(new PairInDecade(pairInDecade.getWord1(),pairInDecade.getWord2(),pairInDecade.getDecade()),new AmountCw1(amount.getAmount(),cw1));
                }
            }

        }
    }
}
