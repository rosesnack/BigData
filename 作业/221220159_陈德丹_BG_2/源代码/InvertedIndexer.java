package org.example;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexer {
    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // default RecordReader: LineRecordReader; key: line offset; value: line string
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            if (fileName.equals("cn_stopwords.txt"))  return;

            Text word = new Text();
            StringTokenizer itr = new StringTokenizer(value.toString());
            for (; itr.hasMoreTokens(); ) {
                word.set(itr.nextToken());
                context.write(word, new Text(fileName));  // 词语作为key，文档名作为value
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> it = values.iterator();
            HashMap<String, Integer> countMap = new HashMap<>();  // {文档名，词语出现次数}
            int sum = 0;  // 词语在所有文档中出现的总次数
            int num = 0;  // 包含该词语的文档数
            while (it.hasNext()) {
                sum++;
                String docName = it.next().toString();
                if (countMap.containsKey(docName)) {
                    countMap.put(docName, countMap.get(docName) + 1);  // 词语在该文档中出现的次数加1
                }
                else {
                    countMap.put(docName,1);  // 词语在该文档中第一次出现
                    num++;  // 包含该词语的文档数加1
                }
            }

            StringBuilder all = new StringBuilder();
            float avg = (float) sum / num; // 平均出现次数
            String formatted = String.format("%.2f", avg); // 保留两位小数
            all.append(formatted).append(", ");

            for (Map.Entry<String, Integer> entry : countMap.entrySet()) { // 遍历文档名和该文档内的词语出现次数
                String docName = entry.getKey();
                int count = entry.getValue();
                all.append(docName).append(":").append(count).append("; ");  // 每个文档的记录
            }

            // 删除最后一个多余的分号和空格
            if (!countMap.isEmpty()) {
                all.setLength(all.length() - 2);
            }

            String docName = "[" + key.toString() + "]";
            context.write(new Text(docName), new Text(all.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        try{
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "InvertedIndexer");
            job.setJarByClass(InvertedIndexer.class);
            job.setMapperClass(InvertedIndexer.InvertedIndexMapper.class);
            job.setReducerClass(InvertedIndexer.InvertedIndexReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch (Exception e) { e.printStackTrace(); }
    }
}
