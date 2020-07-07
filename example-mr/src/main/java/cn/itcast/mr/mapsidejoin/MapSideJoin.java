package cn.itcast.mr.mapsidejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Allen Woon
 */
public class MapSideJoin {
    static class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        //用来缓存小文件（商品文件中的数据）
        Map<String, String> produceMap = new HashMap<String,String>();
        Text k = new Text();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            //将商品文件中的数据写到缓存中  千万别写成/product.data否则会提示找不到该文件
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("pdts.txt")));
            String line = null;
            while((line=br.readLine())!=null){
                //一行数据格式为P0001,xiaomi（商品id，商品名称）
                String[] fields = line.split(",");
                produceMap.put(fields[0], fields[1]);
            }
        }
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //一行订单数据    格式为 1001,20170710,P0001,1（订单id，创建时间，商品id，购买商品数量）
            String line = value.toString();
            String[] fields = line.split(",");
            //根据订单数据中商品id在缓存中找出来对应商品信息(商品名称)，进行串接
            String productName = produceMap.get(fields[2]);
            k.set(line+"\t"+productName);
            context.write(k, NullWritable.get());
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //jar包位置
        job.setJarByClass(MapSideJoin.class);

        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //指定需要缓存一个文件到所有的maptask运行节点工作目录
//      job.addArchiveToClassPath(archive);缓存jar包到task运行节点的classpath中
//      job.addCacheArchive(uri);缓存压缩包到task运行节点的工作目录
//      job.addFileToClassPath(file);//缓存普通文件到task运行节点的classpath中

        //将产品表文件缓存到task工作节点的工作目录中去
        //缓存普通文件到task运行节点的工作目录(hadoop帮我们完成)
//        job.addCacheFile(new URI("/mapjoincache/pdts.txt"));

        //不需要reduce，那么也就没有了shuffle过程
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path("/test/mapjoininput/"));
        FileOutputFormat.setOutputPath(job, new Path("/test/mapjoinoutput2"));

        boolean ex = job.waitForCompletion(true);
        System.exit(ex?0:1);
    }
}
