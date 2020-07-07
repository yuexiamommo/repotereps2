package cn.test.mr.topn;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Allen Woon
 */
public class OrderCountTopReducer extends Reducer<OrderBean,DoubleWritable, OrderBean,DoubleWritable> {

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
    }
}
