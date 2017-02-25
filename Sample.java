import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
 
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
 
public class Sample implements Writable{
    private static final Log log=LogFactory.getLog(Sample.class);
    public static final int DIMENTION=60;
    public double arr[];
     
    public Sample(){
        arr=new double[DIMENTION];
    }
     
    public static double getEulerDist(Sample vec1,Sample vec2){
        if(!(vec1.arr.length==DIMENTION && vec2.arr.length==DIMENTION)){
            log.error("vector's dimention is not "+DIMENTION);
            System.exit(1);
        }
        double dist=0.0;
        for(int i=0;i<DIMENTION;++i){
            dist+=(vec1.arr[i]-vec2.arr[i])*(vec1.arr[i]-vec2.arr[i]);
        }
        return Math.sqrt(dist);
    }
     
    public void clear(){
        for(int i=0;i<arr.length;i++)
            arr[i]=0.0;
    }
     
    @Override
    public String toString(){
        String rect=String.valueOf(arr[0]);
        for(int i=1;i<DIMENTION;i++)
            rect+="\t"+String.valueOf(arr[i]);
        return rect;
    }
 
    @Override
    public void readFields(DataInput in) throws IOException {
        String str[]=in.readUTF().split("\\s+");
        for(int i=0;i<DIMENTION;++i)
            arr[i]=Double.parseDouble(str[i]);
    }
 
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.toString());
    }
}
