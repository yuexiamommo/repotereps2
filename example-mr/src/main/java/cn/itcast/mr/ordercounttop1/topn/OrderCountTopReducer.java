package cn.itcast.mr.ordercounttop1.topn;

import cn.itcast.mr.ordercounttop1.OrderBean;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Allen Woon
 */
public class OrderCountTopReducer extends Reducer<OrderBean,DoubleWritable,OrderBean,DoubleWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        int num = 0;
        for (DoubleWritable value : values) {
            if(num < 2 ){
                context.write(key,value);
                num ++;
            }else {
                break;
            }
        }

        //todo 1、如果不迭代values 直接输出key
//        context.write(key,new DoubleWritable(12.45));
        //todo 2、边迭代 边输出key
//        for (DoubleWritable value : values) {
//            System.out.println(key);
//            context.write(key,value);
//        }
        //todo 3、迭代完 最终输出一次key
//        for (DoubleWritable value : values) {
//        }
//        context.write(key,new DoubleWritable(12.45));
    }
}
