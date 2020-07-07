package cn.itcast.mr.flowcountsort;

import cn.itcast.mr.flowcount.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Allen Woon
 */
public class FlowCountSortReducer extends Reducer<FlowBean,Text,Text,FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //todo 如果迭代器中只有一个元素 请使用下述api进行提取
        Text phoNum = values.iterator().next();
        context.write(phoNum,key);
    }
}
