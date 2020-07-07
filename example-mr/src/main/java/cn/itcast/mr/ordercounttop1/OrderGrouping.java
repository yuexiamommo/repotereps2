package cn.itcast.mr.ordercounttop1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by Allen Woon
 * todo  mapreduce中 自定义分组组件  用于控制reduce阶段 key是否一样
 */
public class OrderGrouping extends WritableComparator {

    //允许通过反射构造对象的实例
    protected OrderGrouping(){
        super(OrderBean.class,true);
    }

    //根据订单的id进行分组 如果id不一样 就不是同一个分组
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean abena = (OrderBean) a;
        OrderBean bbena = (OrderBean) b;
        return abena.getOrder_id().compareTo(bbena.getOrder_id());
//        return  abena.getOrder_id() - bbena.getOrder_id() > 0 ? 1:(abena.getOrder_id()-bbena.getOrder_id() < 0 ? -1 :0);
    }
}
