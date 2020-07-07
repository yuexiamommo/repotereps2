package cn.itcast.mr.flowcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Allen Woon
 */
public class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean> {

    FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long totalUpFlow = 0;
        long totalDownFlow = 0;
        for (FlowBean value : values) {
            totalUpFlow += value.getUpFlow();
            totalDownFlow += value.getDownFlow();
        }
        v.set(totalUpFlow,totalDownFlow);
        //直接输出该用户的流量记录
        context.write(key,v);
    }
}
