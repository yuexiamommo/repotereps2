package cn.itcast.mr.weblog.preprocess;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Allen Woon
 */
public class WebLogMapper extends Mapper<LongWritable,Text,WebLogBean,NullWritable> {


    WebLogBean k = new WebLogBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] fields = line.split(" ");

        if(fields.length >11) {
            //有效的数据
//            k.setValid(true);
            k.setRemote_addr(fields[0]);
            k.setRemote_addr(fields[0]);
            k.setRemote_addr(fields[0]);
            k.setRemote_addr(fields[0]);
            k.setRemote_addr(fields[0]);
            k.setRemote_addr(fields[0]);
            k.setRemote_addr(fields[0]);
            k.setRemote_addr(fields[0]);
            k.setRemote_addr(fields[0]);
            k.setRemote_addr(fields[0]);
        }else {
            //无效的数据
            k.setValid(false);
        }

        context.write(k,NullWritable.get());
    }
}
