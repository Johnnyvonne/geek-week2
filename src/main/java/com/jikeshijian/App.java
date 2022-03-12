package com.jikeshijian;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Hello world!
 *
 */
public class App 
{


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: phone <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "phone");
        job.setJarByClass(App.class);
        job.setMapperClass(PhoneMapper.class);
        job.setCombinerClass(PhoneReduce.class);
        job.setReducerClass(PhoneReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneBean.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs[otherArgs.length - 2]));
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
