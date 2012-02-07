package com.hmsonline.virgil.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("rawtypes")
public class Emitter {
    Text key;
    ObjectWritable value;
    Context context;

    public Emitter(Context context) {
        this.context = context;
        this.value = new ObjectWritable();
        this.key = new Text();
    }

    @SuppressWarnings("unchecked")
    public void emit(String key, Object value) throws IOException, InterruptedException {
        this.value.set(value);
        this.key.set(key);
        context.write(this.key, this.value);
    }
}
