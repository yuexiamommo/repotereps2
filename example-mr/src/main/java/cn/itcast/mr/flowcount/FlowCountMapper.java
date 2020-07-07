package cn.itcast.mr.flowcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Allen Woon
 */
public class FlowCountMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //一行转为String
        String line = value.toString();
        //按照分隔符进行切割
        String[] fields = line.split("\t");
        //提取手机号
        String phoNum = fields[1];
        //提取上下行流量  倒着提取  中间有字段丢失问题
        long upFlow =Long.parseLong(fields[fields.length-3]);
        long downFlow =Long.parseLong(fields[fields.length-2]);
        k.set(phoNum);
        v.set(upFlow,downFlow);
        context.write(k,v);

    }
}
