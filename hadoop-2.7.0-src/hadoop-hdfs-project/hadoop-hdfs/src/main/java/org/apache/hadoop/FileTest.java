package org.apache.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FileTest {

    public static void main(String[] args) throws IOException {
        FileSystem fileSystem = FileSystem.newInstance(new Configuration());


        /**
         * 创建目录 => 修改元数据 => NameNode
         */
        fileSystem.mkdirs(new Path(""));


        /**
         * 写数据
         */
        FSDataOutputStream fsous = fileSystem.create(new Path("/tmp/tmp.txt"));
        // write方法是在FSDataOutputStream的内部类PositionCache中实现的
        fsous.write("dddd".getBytes(StandardCharsets.UTF_8));

    }
}
