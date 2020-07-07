package cn.test.mr.topn;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Allen Woon
 */
public class OrderCountTopMapper extends Mapper<LongWritable,Text, OrderBean,DoubleWritable> {

    OrderBean k = new OrderBean();
    DoubleWritable v = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        //提取每一行数据中的订单编号 订单金额
        k.setOrder_id(fields[0]);
        k.setPrice(Double.parseDouble(fields[2]));

        v.set(Double.parseDouble(fields[2]));

        context.write(k,v);
    }
}
