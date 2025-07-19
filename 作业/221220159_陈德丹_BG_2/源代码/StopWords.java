package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class StopWords {
    public static class StopWordsMapper extends Mapper<Object, Text, Text, Text> {
        private Set<String> patternsToSkip = new HashSet<String>();
        private BufferedReader fis;

        public void setup(Context context) throws IOException, InterruptedException {
            //String fileName = "/home/nightheron/dataset/cn_stopwords.txt";
            Configuration conf = context.getConfiguration();
            URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
            for (URI patternsURI : patternsURIs) {
                Path patternsPath = new Path(patternsURI.getPath());
                String fileName = patternsPath.getName().toString();
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            }
        }

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
                if (!patternsToSkip.contains(word.toString())) {
                    context.write(word, new Text(fileName));
                }
            }
        }
    }

    public static class StopWordsReducer extends Reducer<Text, Text, Text, Text> {
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
                    countMap.put(docName, countMap.get(docName) + 1);
                }
                else {
                    countMap.put(docName,1);
                    num++;
                }
            }

            StringBuilder all = new StringBuilder();
            float avg = (float) sum / num;
            String formatted = String.format("%.2f", avg);
            all.append(formatted).append(", ");

            for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
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
            Job job = Job.getInstance(conf, "StopWords");
            job.setJarByClass(StopWords.class);
            job.setMapperClass(StopWords.StopWordsMapper.class);
            job.setReducerClass(StopWords.StopWordsReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.addCacheFile(new Path(args[2]).toUri());
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch (Exception e) { e.printStackTrace(); }
    }
}
