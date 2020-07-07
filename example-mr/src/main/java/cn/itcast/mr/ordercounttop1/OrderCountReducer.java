package cn.itcast.mr.ordercounttop1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Allen Woon
 */
public class OrderCountReducer extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //没有进行values迭代 直接输出该分组中第一个key 也就是排序的时候订单金额最大哪一个
        context.write(key,NullWritable.get());
    }
}
