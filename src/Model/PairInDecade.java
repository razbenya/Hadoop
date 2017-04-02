package Model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by snoop_000 on 28/02/2017.
 */
public class PairInDecade implements WritableComparable<PairInDecade> {

    protected String word1;
    protected String word2;
    protected int decade;

    public PairInDecade() {
        word1 = "non";
        word2 = "non";
        decade = -1;
    }

    public PairInDecade(String word1, String word2, int decade){
        this.word1 = word1;
        this.word2 = word2;
        this.decade = decade;
    }

    public String getWord1() {
        return word1;
    }

    public void setWord1(String word1) {
        this.word1 = word1;
    }

    public String getWord2() {
        return word2;
    }

    public void setWord2(String word2) {
        this.word2 = word2;
    }

    public int getDecade() {
        return decade;
    }

    public void setDecade(int decade) {
        this.decade = decade;
    }

    public PairInDecade(String Serialization) {
        String[] toks = Serialization.split("\t");
        word1 = toks[0].split(" ")[0];
        word2 = toks[0].split(" ")[1];
        decade = Integer.parseInt(toks[1]);
    }
    @Override
    public int compareTo(PairInDecade o) {

        int temp = decade - o.decade;
        if(temp != 0)
            return temp;
        else {
            if(word1.equals("*"))
                return -1;
            else if(o.word1.equals("*"))
                return 1;
            temp = word1.compareTo(o.word1);
            if(temp != 0)
                return temp;
            else{
                if(word2.equals("*"))
                    return -1;
                else if(o.word2.equals("*"))
                    return 1;
                else
                    return word2.compareTo(o.word2);
            }

        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word1);
        dataOutput.writeUTF(word2);
        dataOutput.writeInt(decade);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word1 = dataInput.readUTF();
        word2 = dataInput.readUTF();
        decade = dataInput.readInt();
    }
    public String toString() {
        return   word1 + " " + word2 + "\t" + decade ;
    }
}

