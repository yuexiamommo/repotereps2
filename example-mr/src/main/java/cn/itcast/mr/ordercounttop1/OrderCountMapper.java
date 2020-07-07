package cn.itcast.mr.ordercounttop1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Allen Woon
 */
public class OrderCountMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable> {

    OrderBean k = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        //提取每一行数据中的订单编号 订单金额
//        k.setOrder_id(Integer.parseInt(fields[0]));
//        k.setPrice(Double.parseDouble(fields[2]));

        context.write(k,NullWritable.get());
    }
}
