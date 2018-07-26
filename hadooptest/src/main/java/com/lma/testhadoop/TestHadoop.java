package com.lma.testhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

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
}
