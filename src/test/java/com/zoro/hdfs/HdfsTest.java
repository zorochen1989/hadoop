package com.zoro.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HdfsTest {

    Configuration conf;
    FileSystem fs;

    @Before
    public void init() throws IOException {
        // 如果在win上运行，必须加上这句话
        System.setProperty("root", "root");
        conf = new Configuration(true);
        fs = FileSystem.get(conf);
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void list() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus f : fileStatuses) {
            String s = f.toString();
            System.out.println(s);
        }
    }

    @Test
    public void del() throws IOException {
        fs.delete(new Path("/hadoop/"),false);
    }
}
