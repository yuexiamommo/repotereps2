package cn.itcast.mr.orderjoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Allen Woon
 */
public class OrderJoinBean implements Writable {
    private String id;
    private String date;
    private String pid;
    private String amount;
    private String name;
    private String categoryId;
    private String price;
    @Override
    public String toString() {
        return id+"\t"+date+"\t"+pid+"\t"+amount+"\t"+name+"\t"+categoryId+"\t"+price;
    }
    public OrderJoinBean() {
    }

    public OrderJoinBean(String id, String date, String pid, String amount, String name, String categoryId, String price) {
        this.id = id;
        this.date = date;
        this.pid = pid;
        this.amount = amount;
        this.name = name;
        this.categoryId = categoryId;
        this.price = price;
    }
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDate() {
        return date;
    }
    public void setDate(String date) {
        this.date = date;
    }
    public String getPid() {
        return pid;
    }
    public void setPid(String pid) {
        this.pid = pid;
    }
    public String getAmount() {
        return amount;
    }
    public void setAmount(String amount) {
        this.amount = amount;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getCategoryId() {
        return categoryId;
    }
    public void setCategoryId(String categoryId) {
        this.categoryId = categoryId;
    }
    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id+"");
        out.writeUTF(date+"");
        out.writeUTF(pid+"");
        out.writeUTF(amount+"");
        out.writeUTF(name+"");
        out.writeUTF(categoryId+"");
        out.writeUTF(price+"");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id =  in.readUTF();
        this.date =  in.readUTF();
        this.pid =  in.readUTF();
        this.amount =  in.readUTF();
        this.name =  in.readUTF();
        this.categoryId =  in.readUTF();
        this.price =  in.readUTF();

    }
}
