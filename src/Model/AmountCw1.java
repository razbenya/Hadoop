package Model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by snoop_000 on 28/02/2017.
 */
public class AmountCw1 extends Amount {

    protected int cw1;

    public AmountCw1(){
        super();
        cw1 = -1;
    }
    public AmountCw1 (int amount, int cw1){
        super(amount);
        this.cw1 = cw1;
    }

    public AmountCw1(String Serialization) {
        super();
        String[] toks = Serialization.split("\t");
        amount = Integer.parseInt(toks[2]);
        cw1 = Integer.parseInt(toks[3]);
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(amount);
        dataOutput.writeInt(cw1);
    }

    public int getCw1() {
        return cw1;
    }

    public void setCw1(int cw1) {
        this.cw1 = cw1;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        amount = dataInput.readInt();
        cw1 = dataInput.readInt();
    }
    public String toString() {
        return  amount+ "\t" +  cw1 ;
    }

}
