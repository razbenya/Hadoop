package Model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Neta on 2017-03-08.
 */
public class FinalValue implements Writable {
    private String word1;
    private String word2;
    private double npmi;

    public FinalValue(){
        word1 ="";
        word2="";
        npmi = -1;
    }


    public FinalValue(String word1, String word2, double npmi) {
        this.word1 = word1;
        this.word2 = word2;
        this.npmi = npmi;
    }

    public String getWord1() {
        return word1;
    }

    public String getWord2() {
        return word2;
    }

    public double getNpmi() {
        return npmi;
    }

    public String toString(){
        return word1+" "+word2+" "+npmi;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word1);
        dataOutput.writeUTF(word2);
        dataOutput.writeDouble(npmi);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word1 = dataInput.readUTF();
        word2 = dataInput.readUTF();
        npmi = dataInput.readDouble();
    }
}
