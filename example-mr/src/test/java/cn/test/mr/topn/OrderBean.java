package cn.test.mr.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Allen Woon
 */
public class OrderBean implements WritableComparable<OrderBean>{

    private String order_id;//订单编号

    private Double price;//订单金额

    public OrderBean() {
    }

    public OrderBean(String order_id, Double price) {
        this.order_id = order_id;
        this.price = price;
    }

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return  order_id +"\t"+price;
    }

    //todo 根据订单编号进行排序 如果订单编号一样 根据价格的倒序排序
    @Override
    public int compareTo(OrderBean o) {
        int result = this.order_id.compareTo(o.getOrder_id());
        if(result == 0){
            return this.price > o.getPrice() ? -1 :1;
        }
        return  result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(order_id);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.order_id = in.readUTF();
        this.price = in.readDouble();
    }
}
