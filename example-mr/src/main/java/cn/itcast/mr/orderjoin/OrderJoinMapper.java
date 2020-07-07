package cn.itcast.mr.orderjoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by Allen Woon
 */
public class OrderJoinMapper extends Mapper<LongWritable,Text,Text,OrderJoinBean>{

    OrderJoinBean orderJoinBean = new OrderJoinBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //通过上下文获取当前操作的切片
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        //通过切片获取到所对应的文件名
        String fileName = fileSplit.getPath().getName();
        System.out.println("当前操作的文件名称是："+fileName);

        //切割value
        String[] fields = value.toString().split(",");
        if (fileName.contains("orders")){
            //订单数据
            orderJoinBean.setId(fields[0]);
            orderJoinBean.setDate(fields[1]);
            orderJoinBean.setPid(fields[2]);
            orderJoinBean.setAmount(fields[3]);

            //输出
            context.write(new Text(fields[2]),orderJoinBean);
        }else {
            //商品数据
            orderJoinBean.setName(fields[1]);
            orderJoinBean.setCategoryId(fields[2]);
            orderJoinBean.setPrice(fields[3]);

            //输出
            context.write(new Text(fields[0]),orderJoinBean);
        }

    }
}
