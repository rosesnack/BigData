package org.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.FloatWritable;

public class Sort {
    public static class SortMapper extends Mapper<Object, Text, FloatWritable, Text>
    {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            // parts[0] is the term, parts[1] is the average, parts[2] are the files
            String[] parts = value.toString().split(",|\\s+", 3);
            FloatWritable average = new FloatWritable(Float.parseFloat(parts[1]));
            Text term = new Text(parts[0] + "," + parts[2]);
            context.write(average, term);
        }
    }

    public static class DescendingFloatComparator extends FloatWritable.Comparator
    {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
        {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
        @Override
        public int compare(Object a, Object b)
        {
            return -super.compare((WritableComparable)a, (WritableComparable)b);
        }
    }

    public static class SortReducer extends Reducer<FloatWritable, Text, Text, Text>
    {
        @Override
        public void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            for (Text value : values)
            {
                String[] parts = value.toString().split(",", 2);
                String new_key = parts[0];
                String new_value = key + ", " + parts[1];
                context.write(new Text(new_key), new Text(new_value));
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sort");
        job.setSortComparatorClass(DescendingFloatComparator.class);
        job.setJarByClass(Sort.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}