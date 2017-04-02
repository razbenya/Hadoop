package Model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by snoop_000 on 28/02/2017.
 */
public class Amount implements Writable {

    protected int amount;

    public Amount(){
        amount = 0;
    }
    public Amount (int amount){
        this.amount = amount;
    }

    public Amount(String Serialization) {
        String[] toks = Serialization.split("\t");
        amount = Integer.parseInt(toks[2]);
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(amount);
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        amount = dataInput.readInt();
    }
    public String toString() {
        return   amount+"";
    }
}
