package Model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by snoop_000 on 01/03/2017.
 */
public class Npmi implements WritableComparable<Npmi> {

    private double npmi;

    public Npmi(String Serialization) {
        String[] toks = Serialization.split("\t");
        npmi = Double.parseDouble(toks[2]);
    }

    public Npmi(double npmi){
        this.npmi = npmi;
    }

    public Npmi(){
        npmi = -1;
    }

    public double getNpmi(){
        return npmi;
    }


    public Npmi(double cw1w2, double N, double cw1 , double cw2){
        double pw1w2 = cw1w2/N;
        if(pw1w2 == 1)
            npmi = 0;
        else {
            double pmi = Math.log(cw1w2) + Math.log(N) - Math.log(cw1) - Math.log(cw2);
            npmi = pmi / (-Math.log(pw1w2));
        }
    }

    @Override
    public int compareTo(Npmi o) {
        return (int)(this.npmi - o.npmi);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(npmi);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        npmi = dataInput.readDouble();
    }
    public String toString(){
        return npmi+"";
    }
}
