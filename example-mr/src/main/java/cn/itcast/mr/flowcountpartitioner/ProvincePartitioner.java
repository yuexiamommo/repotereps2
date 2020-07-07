package cn.itcast.mr.flowcountpartitioner;

import cn.itcast.mr.flowcount.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    
    
    public static HashMap<String, Integer>  provinceMap = new HashMap<String, Integer>();
    
    static{
        provinceMap.put("134", 0);
        provinceMap.put("135", 1);
        provinceMap.put("136", 2);
        provinceMap.put("137", 3);
        provinceMap.put("138", 4);
    }

    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {

        Integer code = provinceMap.get(key.toString().substring(0, 3));
        
        if (code != null) {
            return code;
        }
        
        return 5;
    }

}
