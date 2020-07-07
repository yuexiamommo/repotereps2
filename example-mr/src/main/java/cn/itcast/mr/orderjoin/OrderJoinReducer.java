package cn.itcast.mr.orderjoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by Allen Woon
 */
public class OrderJoinReducer extends Reducer<Text,OrderJoinBean,OrderJoinBean,NullWritable> {
    
    OrderJoinBean orderJoinBean = new OrderJoinBean();
    
    @Override
    protected void reduce(Text key, Iterable<OrderJoinBean> values, Context context) throws IOException, InterruptedException {
        //todo 一组迭代中是同一个商品的所有交易记录 进行拼接即可
        for (OrderJoinBean value : values) {
            System.out.println(value.getId());

            //如果id不为空  说明是 订单数据 提取订单信息
            if(null != value.getId() && !"null".equals(value.getId())){
                orderJoinBean.setId(value.getId());
                orderJoinBean.setDate(value.getDate());
                orderJoinBean.setPid(value.getPid());
                orderJoinBean.setAmount(value.getAmount());
            }else{//如果id 为空 说明是商品数据 提取商品信息
                orderJoinBean.setName(value.getName());
                orderJoinBean.setCategoryId(value.getCategoryId());
                orderJoinBean.setPrice(value.getPrice());
            }
        }
        context.write(orderJoinBean, NullWritable.get());
    }
}
