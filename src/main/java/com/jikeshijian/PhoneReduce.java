package com.jikeshijian;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhoneReduce extends Reducer<Text, PhoneBean, Text, PhoneBean> {

    public void reduce(Text key, Iterable<PhoneBean> values,
                       Context context
    ) throws IOException, InterruptedException {
        long up = 0;
        long down = 0;
        long sum = 0;
        for (PhoneBean val : values) {
            up += val.getUp();
            down += val.getDown();
            sum += val.getTotal();
        }
        PhoneBean pb = new PhoneBean();
        pb.setUp(up);
        pb.setDown(down);
        pb.setTotal(sum);
        context.write(key, pb);
    }
}
