import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;
import java.net.URI;
 
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
public class KMeans extends Configured{
    private static final Log log = LogFactory.getLog(KMeans.class);
 
    private static final int K = 10;                //质心个数
     
    public static class ClusterMapper extends Mapper<LongWritable, Text, IntWritable, Sample> {
        Vector<Sample> centers = new Vector<Sample>();

        // 读取DistributedCache中的质心文件
        @Override
        public void setup(Context context){
	    try{
                URI []caches=DistributedCache.getCacheFiles(context.getConfiguration());
                if(caches==null || caches.length<=0){
                    log.error("data文件不存在");
                    System.exit(1);
                }
                BufferedReader br=new BufferedReader(new FileReader(caches[0].toString()));

                for (int i = 0; i < K; i++)
                    centers.add(new Sample());

                String line;
                while((line=br.readLine())!=null){
                    String []str=line.split("\\s+");
                    if(str.length!=Sample.DIMENTION+1){
                        log.error("读入centers时维度不对");
                        System.exit(1);
                    }

                    int index=Integer.parseInt(str[0]);
                    if(index < 0 && index > 9){
                        log.error("质心编号错误");
                        System.exit(1);
                    }
                    for(int i=1;i<str.length;i++)
                        centers.get(index).arr[i-1]=Double.parseDouble(str[i]);
                }
	    }catch(IOException e){
	    	log.error("出现异常");
		System.exit(1);
	    }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String []str=value.toString().split("\\s+");
            if(str.length != Sample.DIMENTION){
                log.error("读入样本时维度不对");
                System.exit(1);
            }

            Sample sample=new Sample();
            for(int i =0;i<Sample.DIMENTION;++i)
                sample.arr[i]=Double.parseDouble(str[i]);

            int index = -1;
            double minDist=Double.MAX_VALUE;
            for(int i=0;i<K;++i){
                double dist = Sample.getEulerDist(sample,centers.get(i));
                if(dist < minDist){
                    minDist = dist;
                    index = i;
                }
            }

            context.write(new IntWritable(index), sample);
        }
    }
     
    public static class UpdateCenterReducer extends Reducer<IntWritable, Sample, IntWritable, Sample> {

        @Override
        //更新每个质心（除最后一个）
        public void reduce(IntWritable key,Iterable<Sample> values,Context context) throws IOException,InterruptedException{
            Sample center = new Sample();
            int count = 0;
            center.clear();
            for(Sample value : values){
                for(int i=0;i<Sample.DIMENTION;++i)
                    center.arr[i] += value.arr[i];
                ++count;
            }
            for(int i=0;i<Sample.DIMENTION;++i)
                center.arr[i] /= count;

            context.write(key,center);
        }
    }
 
    public static void main(String[] args) throws Exception {
	if(args.length != 3){
	    log.error("Usage: InputDir CacheDir OutputDir");
	    System.exit(1);
	}

        Configuration conf = new Configuration();
        DistributedCache.addCacheFile(new URI(args[1]), conf);
        Job job=new Job(conf,"KMeans");
        job.setJarByClass(KMeans.class);
         
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
         
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(ClusterMapper.class);
        job.setReducerClass(UpdateCenterReducer.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Sample.class);
         
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
