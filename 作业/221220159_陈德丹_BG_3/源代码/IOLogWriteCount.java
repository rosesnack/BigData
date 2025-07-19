import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IOLogWriteCount {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text userNamespace = new Text();          // 用作输出的 key：用户命名空间
        private IntWritable opCount = new IntWritable();  // 用作输出的 value：操作次数

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 将一行按空格切分为字段
            String[] fields = value.toString().split(" ");
            if (fields.length >= 10) { // 保证字段足够，防止越界
                String opName = fields[4]; // 第 4 个字段为操作类型
                if (opName.equals("2")) { // 只处理写操作（op_name == 2）
                    userNamespace.set(fields[5]); // 第 5 个字段为用户命名空间
                    opCount.set(Integer.parseInt(fields[8])); // 第 8 个字段为写操作次数（op_count）
                    context.write(userNamespace, opCount); // 输出 <user_namespace, op_count>
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            // 遍历该命名空间的所有操作次数，加总
            for (IntWritable val : values) {
                sum += val.get();
            }
            // 输出：<user_namespace, sum(op_count)>
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "IOLogWriteCount");
        job.setJarByClass(IOLogWriteCount.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
