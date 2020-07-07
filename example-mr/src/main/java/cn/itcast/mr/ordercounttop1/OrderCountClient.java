package cn.itcast.mr.ordercounttop1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Allen Woon
 */
public class OrderCountClient {
    public static void main(String[] args) throws Exception{
        //配置参数类 用于指定mr运行时相关的参数属性
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "wordcount");

        //指定mr程序运行的主类
        job.setJarByClass(OrderCountClient.class);

        //指定mr程序的mapper reducer类
        job.setMapperClass(OrderCountMapper.class);
        job.setReducerClass(OrderCountReducer.class);

        //指定map阶段输出的kv类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //指定reduce阶段输出的kv类型 也就是mr最终的输出类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //设置自定义分组规则生效
        job.setGroupingComparatorClass(OrderGrouping.class);

        //指定mr程序输入输出的数据路径
        FileInputFormat.addInputPath(job,new Path("D:\\GroupingComparator\\input"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\GroupingComparator\\output1"));

        //提交本mr程序
//        job.submit();
        boolean result = job.waitForCompletion(true);//提交mr程序 并且开启任务执行监控功能
        //如果mr程序执行成功 退出0 否则1
        System.exit(result? 0 : 1);
    }
}
