package com.jikeshijian;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneMapper extends Mapper<Object, Text, Text, PhoneBean> {


    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

        String[] split = value.toString().split("\t");
        if(split.length < 10) {
            return;
        }
        PhoneBean pb = new PhoneBean();
        pb.setUp(Long.parseLong(split[7]));
        pb.setDown(Long.parseLong(split[8]));
        pb.setTotal(pb.getUp() + pb.getDown());
        Text text = new Text(split[1]);
        context.write(text, pb);
    }
}
