package MappersAndReducers;

import Model.AmountCw1;
import Model.Npmi;
import Model.PairInDecade;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by snoop_000 on 01/03/2017.
 */
public class ThirdMapperAndReducer {

    public static class MapperClass extends Mapper<PairInDecade,AmountCw1, PairInDecade,AmountCw1> {
        @Override
        public void map(PairInDecade pairInDecade, AmountCw1 amount, Context context) throws IOException,  InterruptedException {
            context.write(new PairInDecade("*", "*", pairInDecade.getDecade()),amount);
            context.write(new PairInDecade(pairInDecade.getWord2(), "*", pairInDecade.getDecade()),amount);
            context.write(new PairInDecade(pairInDecade.getWord2(),pairInDecade.getWord1(),pairInDecade.getDecade()), amount);
        }
    }

    public static class Reduce extends Reducer<PairInDecade, AmountCw1, PairInDecade, Npmi> {

        private int cw2;
        String currentWord1;
        private int N;
        private int currentDecade;


        protected void setup(Context context) throws IOException, InterruptedException {
            cw2 = 0;
            N = 0;
            currentDecade = 0;
            currentWord1 = "non";
        }

        public void reduce(PairInDecade pairInDecade, Iterable<AmountCw1> values, Context context)
                throws IOException, InterruptedException {
            if(pairInDecade.getDecade() != currentDecade) {
                N = 0;
                cw2 = 0;
                currentDecade = pairInDecade.getDecade();
            }

            if(!pairInDecade.getWord1().equals(currentWord1)) {
                cw2 = 0;
                currentWord1 = pairInDecade.getWord1();
            }
            if(pairInDecade.getWord2().equals("*") && pairInDecade.getWord1().equals("*")) {
                for (AmountCw1 amount : values) {
                    N += amount.getAmount();

                }
            }
            else if(pairInDecade.getWord2().equals("*")) {
                for (AmountCw1 amount : values) {
                    cw2 += amount.getAmount();
                }
            }
            else {
                for (AmountCw1 amount : values) {
                    context.write(new PairInDecade(pairInDecade.getWord2(), pairInDecade.getWord1(),pairInDecade.getDecade()), new Npmi(amount.getAmount(),N, amount.getCw1(),cw2));
                }

            }
        }
    }

}
