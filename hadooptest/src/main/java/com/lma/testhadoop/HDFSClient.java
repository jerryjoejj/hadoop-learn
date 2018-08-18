package com.lma.testhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

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

    /**
     * 展示所有文件
     */
    @Test
    public void testListFiles() throws IOException {

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            //获取名称
            System.out.println(fileStatus.getPath().getName());
            //获取块大小
            System.out.println(fileStatus.getBlockSize());
            //获取文件权限
            System.out.println(fileStatus.getPermission());
            //获取文件大小
            System.out.println(fileStatus.getLen());

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation location: blockLocations) {
                System.out.println("block-length: " + location.getLength() + "--" + "block-offset: " + location.getOffset());

                String[] hosts = location.getHosts();
                for(String host : hosts) {
                    System.out.println(host);
                }
            }
        }

    }

    /**
     * 展示文件及文件夹
     * @throws IOException
     */
    @Test
    public void testListAll() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        String flag = "";

        for (FileStatus fileStat : fileStatuses) {
            if (fileStat.isFile()) {
                flag = "f--";
            } {
                flag = "d--";
            }

            System.out.println(flag + fileStat.getPath().getName());
            System.out.println(fileStat.getPermission());
        }
    }

}
