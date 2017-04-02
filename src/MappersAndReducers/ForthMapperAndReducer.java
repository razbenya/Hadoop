package MappersAndReducers;

import Model.FinalKey;
import Model.FinalValue;
import Model.Npmi;
import Model.PairInDecade;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by snoop_000 on 01/03/2017.
 */
public class ForthMapperAndReducer {

    public static class MapperClass extends Mapper<PairInDecade,Npmi, FinalKey, FinalValue> {
        @Override
        public void map(PairInDecade pairInDecade, Npmi npmi, Context context) throws IOException,  InterruptedException {
            context.write(new FinalKey("a", pairInDecade.getDecade(), npmi.getNpmi()), new FinalValue("","",npmi.getNpmi()));
            context.write(new FinalKey("z",pairInDecade.getDecade(), npmi.getNpmi()),new FinalValue(pairInDecade.getWord1(),pairInDecade.getWord2(),npmi.getNpmi()));
        }
    }

    public static class Reduce extends Reducer<FinalKey, FinalValue, PairInDecade, Npmi> {
        private double totalNpmi;
        private int currentDecade;
        private double minPmi;
        private double relMinPmi;

        protected void setup(Context context) throws IOException, InterruptedException {
            totalNpmi = 0;
            currentDecade = 0;
            minPmi = Double.parseDouble(context.getConfiguration().get("minPmi", "1"));
            relMinPmi = Double.parseDouble(context.getConfiguration().get("relMinPmi", "1"));
        }

        public void reduce(FinalKey finalKey, Iterable<FinalValue> values, Context context) throws IOException, InterruptedException {
            if(finalKey.getDecade() != currentDecade) {
                currentDecade = finalKey.getDecade();
                totalNpmi = 0;
            }
           if(finalKey.getWord().equals("a")) {
                for (FinalValue npmi : values) {
                    totalNpmi += npmi.getNpmi();
                }
            }
            else {
                for (FinalValue npmi : values) {
                   double relativeNpmi = npmi.getNpmi() / totalNpmi;
                   if (npmi.getNpmi() >= minPmi || relativeNpmi >= relMinPmi)
                       context.write(new PairInDecade(npmi.getWord1(), npmi.getWord2(), finalKey.getDecade()), new Npmi(npmi.getNpmi()));
               }
            }
        }
    }
}
