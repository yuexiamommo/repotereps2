package cn.itcast.mr.flowcount;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Allen Woon
 * todo 要想在mr中 把自定义数据类型作为参数传递 需要实现hadoop序列化 接口
 */
public class FlowBean implements WritableComparable<FlowBean>{
    private long upFlow;

    private long downFlow;

    private long totalFlow;

    //todo 快捷键 alt+insert 无参构造  反射构造对象
    public FlowBean() {
    }

    public FlowBean(long upFlow, long downFlow, long totalFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.totalFlow = totalFlow;
    }

    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.totalFlow = upFlow+downFlow;
    }

    public void set(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.totalFlow = upFlow+downFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getTotalFlow() {
        return totalFlow;
    }

    public void setTotalFlow(long totalFlow) {
        this.totalFlow = totalFlow;
    }

    @Override
    public String toString() {
        return upFlow+"\t"+downFlow+"\t"+totalFlow;
    }

    /**
     * 序列化方法
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(totalFlow);
    }

    /**
     *  反序列化方法
     *  todo 注意反序列化的顺序 先进去先出来
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow =  in.readLong();
        this.downFlow =  in.readLong();
        this.totalFlow =  in.readLong();
    }

    /**
     *  todo 自定义排序规则所在的方法  本需求中需要根据总流量倒序排序
     * @param
     * @return
     */
    @Override
    public int compareTo(FlowBean o) {
        //我比你大 依然返回负数 认为比你小 放在前面 实际上比你大 倒序
        return this.getTotalFlow()  >  o.getTotalFlow() ? -1 :1;

    }
}
