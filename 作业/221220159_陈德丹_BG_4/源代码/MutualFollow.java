import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MutualFollow {
    public static class MutualFollowMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text userPair = new Text();
        private final static IntWritable DIRECTION = new IntWritable();
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // 解析输入行，格式如 "user:follow1 follow2 ..."
            String[] parts = value.toString().split(":");
            if (parts.length != 2) return;
            String userA = parts[0].trim();
            int a = Integer.parseInt(userA);  //将输入的用户A转换为整数

            String[] following = parts[1].trim().split("\\s+");
            for (String userB : following) {  // 遍历用户A的关注列表
                if (userB.isEmpty()) continue;
                int b = Integer.parseInt(userB);

                // 确保小号在前，大号在后
                int min = Math.min(a, b);
                int max = Math.max(a, b);
                String pair = min + "-" + max;
                userPair.set(pair);

                // 判断关注方向（标记1或2）
                if (a == min) {
                    DIRECTION.set(1); // 小号关注大号
                } else {
                    DIRECTION.set(2); // 大号关注小号
                }
                context.write(userPair, DIRECTION);
            }
        }
    }

    // Reducer：检查是否存在双向标记
    public static class MutualFollowReducer
            extends Reducer<Text, IntWritable, Text, NullWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            boolean hasForward = false;
            boolean hasReverse = false;

            for (IntWritable val : values) {
                if (val.get() == 1) hasForward = true;
                else if (val.get() == 2) hasReverse = true;

                // 提前终止循环优化性能
                if (hasForward && hasReverse) break;
            }

            // 输出互相关注的用户对
            if (hasForward && hasReverse) {
                context.write(key, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "MutualFollow");
            job.setJarByClass(MutualFollow.class);
            job.setMapperClass(MutualFollow.MutualFollowMapper.class);
            job.setReducerClass(MutualFollow.MutualFollowReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}