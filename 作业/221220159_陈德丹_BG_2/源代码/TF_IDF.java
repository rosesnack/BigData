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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

public class TF_IDF {
    public static class TF_IDFMapper extends Mapper<Object, Text, Text, Text> {
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
                context.write(word, new Text(fileName));
            }
        }
    }

    public static class TF_IDFReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> it = values.iterator();
            HashMap<String, Integer> countMap = new HashMap<>();  // {文档名，词语出现次数}
            int num = 0;  // 包含该词语的文档数
            while (it.hasNext()) {
                String docName = it.next().toString();
                if (countMap.containsKey(docName)) {
                    countMap.put(docName, countMap.get(docName) + 1);
                }
                else {
                    countMap.put(docName,1);
                    num++;
                }
            }

            Configuration conf = context.getConfiguration();
            int fileCount = conf.getInt("fileCount", 0);
            double IDF = Math.log((double)fileCount / (num + 1)) / Math.log(2);

            // 为每个文档计算TF-IDF
            for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
                String docName = entry.getKey();
                int TF = entry.getValue();
                double TF_IDF = TF * IDF;
                Text value = new Text(String.format("%.2f, ", TF_IDF));
                context.write(new Text(docName + "[" + key.toString() + "]"), value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        try{
            Configuration conf = new Configuration();

            // 增加一个变量存储语料库里文档数目
            Path inputPath = new Path(args[0]);
            FileSystem fs = inputPath.getFileSystem(conf);
            FileStatus[] fileStatuses = fs.listStatus(inputPath);
            int fileCount = fileStatuses.length - 1;
            conf.setInt("fileCount", fileCount);
            fs.close();

            Job job = Job.getInstance(conf, "TF_IDF");
            job.setJarByClass(TF_IDF.class);
            job.setMapperClass(TF_IDF.TF_IDFMapper.class);
            job.setReducerClass(TF_IDF.TF_IDFReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch (Exception e) { e.printStackTrace(); }
    }
}
