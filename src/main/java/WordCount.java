/**
 * Created by yuanfanz on 17/1/15.
 */
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            //key: offset
            //value: line
            //first split the whole line into string array by space
            String[] words = value.toString().split(" ");
            //iterate the string array
            for (String word : words) {
                //写入写出都是以key-value pair的形式
                //key是单词本身，封装在text里面
                Text outKey = new Text(word);
                //value就是count，用IntWritable封装value
                //对于mapper，它只起到了拆分的作用，每次的count都是1，统计相加总数由reducer来完成
                IntWritable outValue = new IntWritable(1);
                //用自带的context方法写出key-value pair
                context.write(outKey, outValue);
            }
        }
    }

    public static class IntSumReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            //I [1]
            //big [1,1]
            //Iterable就是遍历所有的1，再相加
            int sum = 0;
            //iterate values and sum up
            for (IntWritable value : values) {
                sum += value.get();
            }
            //key不变，sum用IntWritable封装
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        //定义MapReduce中的配置，读取写出的一些细节
        Configuration configuration = new Configuration();
        //一个run的project就是一个job
        Job job = Job.getInstance(configuration);
        //告诉job运行哪个class的jar
        job.setJarByClass(WordCount.class);
        //mapper, reducer, output key/value
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //文件输入和输出的path从命令行读取，分别是第一个和第二个
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
