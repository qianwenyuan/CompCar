/**
 * Created by frankqian on 2017/12/25.
 */

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Collect {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text value, Mapper.Context context
        ) throws IOException, InterruptedException {
            //StringTokenizer itr = new StringTokenizer(value.toString());
            //while (itr.hasMoreTokens()) {
            IntWritable one = new IntWritable(1);

            String line = value.toString();
            if (line.length()>0 && line.charAt(0)>='0' && line.charAt(0)<='9') {
                String[] parts = line.split(" ");

                Integer num = (parts.length-2)/2;
                for (Integer i = 0; i < num - 1; i++) {
                    Integer j = i + 1;
                    Integer timei = Integer.parseInt(parts[2 + i * 2 + 1]);
                    Integer timej = Integer.parseInt(parts[2 + j * 2 + 1]);
                    while (j+1<num && timei==timej && Integer.parseInt(parts[2+i*2])==Integer.parseInt(parts[2+j*2])) {
                        i++; j++;
                        timei = Integer.parseInt(parts[2 + i * 2 + 1]);
                        timej = Integer.parseInt(parts[2 + j * 2 + 1]);
                    }
                    while (timej - timei <= 180 && j < num) {
                        if (Integer.parseInt(parts[2 + i * 2]) < Integer.parseInt(parts[2 + j * 2])) {
                            context.write(new Text(parts[2 + i * 2] + " " + parts[2 + j * 2]), one);
                        } else if (Integer.parseInt(parts[2 + i * 2]) > Integer.parseInt(parts[2 + j * 2])) {
                            context.write(new Text(parts[2 + j * 2] + " " + parts[2 + i * 2]), one);
                        }
                        j += 1;
                        if (j < num) {
                            timej = Integer.parseInt(parts[2 + j * 2 + 1]);
                        }
                    }
                }

            }


        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            Integer sum=0;

            for (IntWritable val:values) {
                sum+=val.get();
            }
            //System.out.println(sum);
            if (sum>=10)
                context.write(key, new IntWritable(sum));
            //context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = new Job(conf, "Collect");
        job.setJarByClass(Collect.class);
        job.setMapperClass(TokenizerMapper.class);

        job.setReducerClass(IntSumReducer.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setNumReduceTasks(200);
        //job.setGroupingComparatorClass();
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setMaxInputSplitSize(job,1*1048576);
        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
