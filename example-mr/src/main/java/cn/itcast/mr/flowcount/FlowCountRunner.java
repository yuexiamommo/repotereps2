package cn.itcast.mr.flowcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Allen Woon
 */
public class FlowCountRunner {
    public static void main(String[] args) throws Exception{
        //配置参数类 用于指定mr运行时相关的参数属性
        Configuration conf = new Configuration();

        //配置mr程序本地运行
        conf.set("mapreduce.framework.name","local");

        Job job = Job.getInstance(conf, "flow count");

        //指定mr程序运行的主类
        job.setJarByClass(FlowCountRunner.class);

        //指定mr程序的mapper reducer类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //指定map阶段输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //指定reduce阶段输出的kv类型 也就是mr最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定mr程序输入输出的数据路径
        FileInputFormat.addInputPath(job,new Path("D:\\flowsum\\input"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\flowsum\\output"));

        //提交本mr程序
//        job.submit();
        boolean result = job.waitForCompletion(true);//提交mr程序 并且开启任务执行监控功能
        //如果mr程序执行成功 退出0 否则1
        System.exit(result? 0 : 1);
    }
}
