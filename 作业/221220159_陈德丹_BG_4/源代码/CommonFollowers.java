import java.io.*;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;

public class CommonFollowers {
    public static class CommonFollowersMapper
            extends Mapper<Object, Text, Text, Text> {

        // 存储格式：<用户ID, 该用户参与的所有互相关注用户对>
        // 例如用户1参与的用户对：["1-2", "1-3"]
        private Map<String, List<String>> mutualPairs = new HashMap<>();
        private BufferedReader fis;

        @Override
        protected void setup(Context context) throws IOException {
            // 从分布式缓存读取任务1生成的互相关注用户对
            Configuration conf = context.getConfiguration();
            URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
            for (URI patternsURI : patternsURIs) {
                Path patternsPath = new Path(patternsURI.getPath());
                String fileName = patternsPath.getName().toString();
                fis = new BufferedReader(new FileReader(fileName));
                String line = null;
                while ((line = fis.readLine()) != null) {
                    String pair = line.trim(); // 用户对格式如"1-2"
                    String[] users = pair.split("-");
                    if (users.length != 2) continue; // 跳过格式错误的数据

                    // 将用户对关联到两个用户上，便于后续快速查找
                    // 示例：用户对"1-2"会添加到用户1和用户2的关联列表中
                    mutualPairs.computeIfAbsent(users[0], k -> new ArrayList<>()).add(pair);
                    mutualPairs.computeIfAbsent(users[1], k -> new ArrayList<>()).add(pair);
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // 输入格式示例："1:2 3 4"
            String[] parts = value.toString().split(":");
            if (parts.length != 2) return; // 跳过格式错误的数据

            String currentUser = parts[0].trim();    // 当前用户ID
            String followsList = parts[1].trim();    // 该用户的关注列表

            // 查找当前用户参与的所有互相关注用户对
            List<String> pairs = mutualPairs.get(currentUser);
            if (pairs != null) {
                for (String pair : pairs) {
                    /* 输出格式：
                     * Key   : 用户对（如"1-2"）
                     * Value : "当前用户:关注列表"（如"1:2 3 4"）
                     * 
                     * 目的：将同一用户对的两个用户的关注列表发送到同一个Reducer
                     */
                    context.write(new Text(pair), value);
                }
            }
        }
    }

    // ================================ Reducer 阶段 ================================
    /**
     * Reducer职责：
     * 1. 收集同一用户对的两个用户的关注列表
     * 2. 计算共同关注
     * 3. 根据阈值x选择输出路径
     */
    public static class CommonFollowersReducer
            extends Reducer<Text, Text, Text, Text> {

        private MultipleOutputs<Text, Text> mos; // 多路输出控制器
        private int x;                            // 阈值参数
        private String lessOutputName, greaterOutputName; // 动态输出文件名

        @Override
        protected void setup(Context context) {
            mos = new MultipleOutputs<>(context);
            // 从作业配置中读取参数x（默认值10）
            x = context.getConfiguration().getInt("x", 10); 
            // 动态生成输出文件名（例如x=15 → "less15"和"greater15"）
            lessOutputName = "less" + x;
            greaterOutputName = "greater" + x;
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // 用户对格式如"1-2"
            String[] users = key.toString().split("-");
            String userA = users[0]; // 用户A（小号）
            String userB = users[1]; // 用户B（大号）

            // 存储两个用户的关注列表
            Set<String> followsA = new HashSet<>();
            Set<String> followsB = new HashSet<>();

            // 解析来自Mapper的值（格式："用户ID:关注列表"）
            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts.length != 2) continue; // 跳过格式错误的数据

                String userId = parts[0];       // 用户ID
                String[] follows = parts[1].trim().split("\\s+"); // 分割关注列表

                // 将关注列表按用户归属分类
                if (userId.equals(userA)) {
                    followsA.addAll(Arrays.asList(follows));
                } else if (userId.equals(userB)) {
                    followsB.addAll(Arrays.asList(follows));
                }
            }

            // 计算交集：共同关注
            Set<String> common = new HashSet<>(followsA);
            common.retainAll(followsB);
            if (common.isEmpty()) return; // 无共同关注则跳过

            System.out.println(common.size());
            System.out.println(common);

            // 将共同关注的交集转化为String
            String outputValue = String.join(" ", common);

            // 构造输出键（原用户对追加冒号，如"1-2:"）
            Text outputKey = new Text(key.toString() + ":");

            // 根据共同关注数量选择输出路径
            if (common.size() <= x) {
                mos.write(lessOutputName, outputKey, new Text(outputValue));
            } else {
                mos.write(greaterOutputName, outputKey, new Text(outputValue));
            }
        }

        @Override
        protected void cleanup(Context context) 
                throws IOException, InterruptedException {
            mos.close(); // 关闭多路输出控制器
        }
    }
    public static void main(String[] args) throws Exception {
        try {
            Configuration conf = new Configuration();
            int x = Integer.parseInt(args[2]);
            conf.setInt("x", x); // 将x存入作业配置
            Job job = Job.getInstance(conf, "CommonFollowers");
            job.setJarByClass(CommonFollowers.class);
            job.addCacheFile(new Path(args[1]).toUri());
            // 动态生成输出名称（例如x=15 → "less15"和"greater15"）
            String lessOutput = "less" + x;
            String greaterOutput = "greater" + x;
            // 注册多路输出格式（必须与Reducer中的名称一致）
            MultipleOutputs.addNamedOutput(job, lessOutput, 
                TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, greaterOutput, 
                TextOutputFormat.class, Text.class, Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[3]));
            job.setMapperClass(CommonFollowersMapper.class);
            job.setReducerClass(CommonFollowersReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}