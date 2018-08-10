package com.lma.testhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static java.lang.System.in;

public class HDFSClient {

    FileSystem fs = null;

    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();

        fs = FileSystem.get(new URI("hdfs://h7master1:9000"), conf, "hadoop");
    }


    /**
     * 通过流的形式上传数据到hdfs
     * @throws IOException
     */
    @Test
    public void testUploadFileToHDFS() throws IOException {

        FileInputStream in = new FileInputStream("e://java.pdf");
        FSDataOutputStream out = fs.create(new Path("/java"));

        IOUtils.copyBytes(in, out, 4096);

        fs.close();

    }

    /**
     * 通过流的形式从hdfs下载文件
     */
    @Test
    public void testDownloadFileFromHDFS() throws IOException {
        FSDataInputStream in = fs.open(new Path("/1.txt"));
        FileOutputStream out = new FileOutputStream("d://hello.txt");

        IOUtils.copyBytes(in, out, 4096);
    }

}
