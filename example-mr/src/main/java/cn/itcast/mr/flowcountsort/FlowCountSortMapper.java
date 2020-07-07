package cn.itcast.mr.flowcountsort;

import cn.itcast.mr.flowcount.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Allen Woon
 */
public class FlowCountSortMapper extends Mapper<LongWritable,Text,FlowBean,Text> {

    FlowBean k = new FlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] fields = line.split("\t");
        k.set(Long.parseLong(fields[1]),Long.parseLong(fields[2]));
        v.set(fields[0]);

        context.write(k,v);
    }
}
