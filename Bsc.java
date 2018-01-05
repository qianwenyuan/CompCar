/**
 * Created by frankqian on 2017/12/22.
 */
import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Bsc {
    static class MyComparator implements Comparator {
        public int compare(Object obj1, Object obj2) {
            String a = (String) obj1;
            String b = (String) obj2;

            String[] strs = a.split(" ");
            String[] times = strs[3].split(":");
            Integer time = Integer.parseInt(times[0])*3600+Integer.parseInt(times[1])*60+Integer.parseInt(times[2]);

            String[] strs1 = b.split(" ");
            String[] times1 = strs1[3].split(":");
            Integer time1 = Integer.parseInt(times1[0])*3600+Integer.parseInt(times1[1])*60+Integer.parseInt(times1[2]);

            return time.compareTo(time1);
        }

    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{
        Text road = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
                String[] parts = value.toString().split(",");
                road.set(parts[1]);
                String str=parts[0]+"/";

                for (Integer i=0;i<parts[2].length();++i) {
                    if (parts[2].charAt(i)!=' ')
                        str+=parts[2].charAt(i);
                    else {
                        str+=' ';
                        while(parts[2].charAt(i+1)==' ')
                            ++i;
                    }
                }

                context.write(road, new Text(str));
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            List<String> textlist1 = new ArrayList<String>();
            textlist1.clear();

            for (Text val:values) {
                textlist1.add(val.toString());
            }
            Collections.sort(textlist1,new MyComparator());

            Integer listlen = textlist1.size();

            for (Integer i=0;i<listlen-1;++i) {
                String val = textlist1.get(i);
                Integer j = i+1;
                String val1 = textlist1.get(j);

                String[] strs = val.split("/");
                Integer car = Integer.parseInt(strs[0]);
                String[] strss = strs[1].split(" ");
                String[] times = strss[3].split(":");
                Integer time = Integer.parseInt(times[0])*3600+Integer.parseInt(times[1])*60+Integer.parseInt(times[2]);

                String[] strs1 = val1.split("/");
                Integer car1 = Integer.parseInt(strs1[0]);
                String[] strss1 = strs1[1].split(" ");
                String[] times1 = strss1[3].split(":");
                Integer time1 = Integer.parseInt(times1[0])*3600+Integer.parseInt(times1[1])*60+Integer.parseInt(times1[2]);

                while(Math.abs(time-time1)<=180 && j<listlen) {
                    if (car<car1) {
                        context.write(new Text(car.toString()+" "+car1.toString()), new Text("1"));
                    }
                    else {
                        context.write(new Text(car1.toString()+" "+car.toString()), new Text("1"));
                    }
                    ++j;
                    if (j>=listlen)
                        break;
                    val1 = textlist1.get(j);
                    strs1 = val1.split("/");
                    car1 = Integer.parseInt(strs1[0]);
                    strss1 = strs1[1].split(" ");
                    times1 = strss1[3].split(":");
                    time1 = Integer.parseInt(times1[0])*3600+Integer.parseInt(times1[1])*60+Integer.parseInt(times1[2]);

                }
            }
            //System.out.println(sum);
            //context.write(key, new Text(sum.toString()));
            //context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = new Job(conf, "Bsc");
        job.setJarByClass(Bsc.class);
        job.setMapperClass(TokenizerMapper.class);

        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(10);


        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
