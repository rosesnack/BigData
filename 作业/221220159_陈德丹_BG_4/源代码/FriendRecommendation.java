import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;

public class FriendRecommendation {
    // ====================== Mapper 阶段 ======================
    public static class RecommendationMapper
            extends Mapper<Object, Text, Text, Text> {

        // 建立互相关注列表 存储格式：<用户ID, 与该用户互相关注的所有用户>
        // 例如用户1对应 ["2", "3"]
        private Map<String, Set<String>> mutualFollowers = new HashMap<>();
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

                    // 将用户1添加到用户0的互相关注列表中，将用户0添加到用户1的互相关注列表中
                    mutualFollowers.computeIfAbsent(users[0], k -> new HashSet<>()).add(users[1]);
                    mutualFollowers.computeIfAbsent(users[1], k -> new HashSet<>()).add(users[0]);
                }
            }
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // 输入格式：user:follow1 follow2...
            String[] parts = value.toString().split(":");
            String userB = parts[0].trim();

            //将用户B的关注列表转换为集合
            Set<String> userB_following = new HashSet<>(
                    Arrays.asList(parts[1].trim().split(" "))
            );
            // 获取用户B的互相关注列表
            Set<String> userB_mutual_follow = mutualFollowers.get(userB);
            if (userB_mutual_follow == null) return; // 如果没有互相关注的用户，跳过
            // 遍历用户B的所有互相关注用户
            for (String userA : userB_mutual_follow) {
                // 对于与用户B互相关注的用户A，获取其互相关注列表
                Set<String> userA_mutual_follow = mutualFollowers.get(userA);
                for (String userC : userA_mutual_follow) {
                    if (userB_following.contains(userC)) continue;
                    if (userB.equals(userC)) continue; // 跳过自己

                    // 此时A和B、A和C都互相关注，但B不关注C
                    // 生成推荐关系：给B推荐C
                    // Key: 被推荐人(userB), Value: 候选用户
                    context.write(new Text(userB), new Text(userC));
                }
            }
        }
    }

    // ====================== Reducer 阶段 ======================
    public static class RecommendationReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // 存储候选用户的推荐次数
            Map<String, Integer> recommendations = new HashMap<>();

            // 统计推荐次数
            for (Text val : values) {
                String candidate = val.toString();
                recommendations.put(candidate, recommendations.getOrDefault(candidate, 0) + 1);
            }

            // 构造前5推荐结果
            // 排序并取前 5 个 key，拼接成字符串
            String top5 = recommendations.entrySet().stream()
                    .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue())) // 按 value 倒序
                    .limit(5)
                    .map(Map.Entry::getKey) // 只保留 key
                    .collect(Collectors.joining(" ")); // 用空格拼接

            context.write(key, new Text(top5));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FriendRecommendation");
        job.addCacheFile(new Path(args[1]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setJarByClass(FriendRecommendation.class);
        job.setMapperClass(RecommendationMapper.class);
        job.setReducerClass(RecommendationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}