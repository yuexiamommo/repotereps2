package cn.itcast.mr.orderjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Allen Woon
 */
public class OrderjoinClient {
    public static void main(String[] args) throws Exception {
        //配置参数类
        Configuration conf = new Configuration();

        //获的job的实例对象
        Job job = Job.getInstance(conf);

        //指定mr程序运行的主类
        job.setJarByClass(OrderjoinClient.class);

        //指定mr程序运行的mapper  reducer类
        job.setMapperClass(OrderJoinMapper.class);
        job.setReducerClass(OrderJoinReducer.class);

        //指定map阶段输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderJoinBean.class);

        //指定reduce阶段输出的kv类型 也就是最终的输出类型
        job.setOutputKeyClass(OrderJoinBean.class);
        job.setOutputValueClass(NullWritable.class);

        //指定输入输出的数据路径
        FileInputFormat.addInputPath(job,new Path("D:\\reducejoin\\input"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\reducejoin\\output"));

        //提交job
//        job.submit();
        boolean result = job.waitForCompletion(true);
        //退出程序
        System.exit(result ? 0 :1);
    }
}
