package cn.itcast.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Allen Woon
 *
 * todo 该类就是mr程序中map阶段运行所在的类 也就是maptask
 *
 * KEYIN: 表示map阶段输入kv中k的类型    在默认机制下 是偏移量 long
 * VALUEIN：表示map阶段输入kv中v的类型  在默认机制下 是每行内容 String
 *         todo 在mr中 有默认的读取数据组件 TextinputFormat  一行一行读取数据
 *         把这一行起始的偏移量作为key 这一行内容作为value
 *
 * KEYOUT  表示map阶段输出kv中k的类型  本需求中 是单词 String
 * VALUEOUT 表示map阶段输出kv中v的类型  本需求中 是单词次数1  int
 *
 * todo hadoop认为 java自带的序列化机制 比较垃圾  效率不高  传递臃肿   因此hadoop自己封装了一套类型和序列化机制  Writable
 * long----->longWritable
 * String--->Text
 * int------>intWritable
 * Double--->DoubleWritable
 * null----->nullWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * todo 该方法就是map阶段具体业务逻辑实现的方法所在
     *  该方法被调用多少次 取决于数据有多少行  TextinputFormat读取一行数据 传入一个kv对 调用一次map方法
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.把输入的一行内容变成String
        String line = value.toString(); //hello allen hello allen hello

        //2.根据分隔符切割改行内容
        String[] words = line.split(" ");//[hello,allen,hello,allen,hello]

        //3、遍历数组
        for (String word : words) {
            //4、直接输出<单词，1>
            context.write(new Text(word),new IntWritable(1));//<hello,1><allen,1><hello,1><allen,1><hello,1>
        }
    }
}
