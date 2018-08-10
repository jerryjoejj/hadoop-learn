package com.lma.testhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

import java.io.IOException;

public class TestHadoop {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://h7master1:9000");

        FileSystem fs = FileSystem.get(conf);

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), false);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            Path path = fileStatus.getPath();
            System.out.println(path.getName());
        }

    }

    @Test
    public void uploadFile() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://h7master1:9000");

        // 伪造客户端身份
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        FileSystem fs = FileSystem.get(conf);

        fs.copyFromLocalFile(new Path("E://1.txt"), new Path("/"));

    }

    @Test
    public void downloadFile() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://h7master1:9000");

        // 伪造客户端身份
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        FileSystem fs = FileSystem.get(conf);

//        fs.copyToLocalFile(false, new Path("/1.txt"), new Path("e://"), true);
        fs.copyToLocalFile(new Path("/1.txt"), new Path("e://"));
        fs.close();
    }
}
