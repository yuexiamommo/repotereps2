package cn.itcast.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Allen Woon
 * todo 该类是mr程序运行的客户端主类 主要用于各种参数属性的拼接 指定 最终任务的提交动作
 */
public class WordCountDriver {
    public static void main(String[] args) throws Exception{
        //配置参数类 用于指定mr运行时相关的参数属性
        Configuration conf = new Configuration();
    
//        //开启map输出的压缩 并且指定压缩算法是snappy
//        conf.set("mapreduce.map.output.compress","true");
//        conf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
//
//        //开启reduce输出压缩 最终结果的压缩  snappy
//        conf.set("mapreduce.output.fileoutputformat.compress","true");
//        conf.set("mapreduce.output.fileoutputformat.compress.type","RECORD");
//        conf.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");

        Job job = Job.getInstance(conf, "wordcount");

        //指定mr程序运行的主类
        job.setJarByClass(WordCountDriver.class);

        //指定mr程序的mapper reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //指定map阶段输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定reduce阶段输出的kv类型 也就是mr最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


//        job.setCombinerClass(WordCountReducer.class);

//        job.setNumReduceTasks(3);

        //设置mr程序读取数据的组件为CombineTextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        //设置小文件切片的最大值 上限
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);// 4m

        //指定mr程序输入输出的数据路径
        FileInputFormat.addInputPath(job,new Path("D:\\datasets\\wordcount\\input"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\datasets\\wordcount\\output2"));

        //提交本mr程序
//        job.submit();
        boolean result = job.waitForCompletion(true);//提交mr程序 并且开启任务执行监控功能
        //如果mr程序执行成功 退出0 否则1
        System.exit(result? 0 : 1);
    }
}
