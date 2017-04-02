package Model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by snoop_000 on 01/03/2017.
 */
public class FinalKey implements WritableComparable<FinalKey> {

    private String word;
    private int decade;
    private double npmi;

    public FinalKey() {
        word = "";
        decade = -1;
        npmi = -1;
    }

    public double getNpmi(){
        return npmi;
    }

    public FinalKey(String word , int decade,double npmi){
        this.word = word;
        this.decade = decade;
        this.npmi = npmi;
    }

    public String getWord() {
        return word;
    }



    public int getDecade() {
        return decade;
    }


    @Override
    public int compareTo(FinalKey o) {
        double temp = decade - o.decade;
        if(temp!=0){
            return (int)temp;
        } else {
            temp = word.compareTo(o.word);
            if(temp != 0)
                return (int)temp;
            else {
                return Double.compare(o.npmi , npmi);
            }
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
        dataOutput.writeInt(decade);
        dataOutput.writeDouble(npmi);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word = dataInput.readUTF();
        decade = dataInput.readInt();
        npmi = dataInput.readDouble();
    }
    public String toString() {
        return  decade+"";
    }
}
