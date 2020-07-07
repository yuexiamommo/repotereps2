package cn.itcast.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Allen Woon
 * todo 该类就是mr程序中reduce阶段运行所在的类 也就是reudcetask
 *
 * KEYIN    是reduce阶段输入的kv类型中k 对应着map输出k的类型 本需求中都是单词  Text
 * VALUEIN  是reduce阶段输入的kv类型中v 对应着map输出v的类型 本需求中都是单词的次数1 IntWritable
 *
 * KEYOUT   是reduce阶段输出的kv类型中k  本需求中 是单词 Text
 * VALUEOUT 是reduce阶段输出的kv类型中v  本需求中 是单词的总次数 IntWritable
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    /**
     * todo 该方法就是reducer阶段具体业务逻辑处理的方法
     *      <hello,1><allen,1><hello,1><allen,1><hello,1>
     *  1、排序  默认行为：key的字典序  a--z  0--9
     *     <allen,1><allen,1><hello,1><hello,1><hello,1>
     *  2、分组  默认行为：key相同的分为一组
     *      <allen,1><allen,1>
     *      <hello,1><hello,1><hello,1>
     *  3、一组调用一次reduce方法
     *      在一个组内：组合成为一个新的kv对：todo :key是该组共同的key  value是该组所有value组成的一个数据结构 迭代器Iterable
     *       <allen,1><allen,1>------>  <allen,[1,1]>
     *       <hello,1><hello,1><hello,1>---->  <hello,[1,1,1]>
     *       <hive,1>----->   <hive,[1]>
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        //遍历该组的所有values构成的迭代器
        for (IntWritable value : values) {//<allen,[1,1]>
            count += value.get();
        }
        //直接输出结果
        context.write(key,new IntWritable(count));//  <allen,2>
    }
}
