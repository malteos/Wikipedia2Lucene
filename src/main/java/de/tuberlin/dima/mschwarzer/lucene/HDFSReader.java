package de.tuberlin.dima.mschwarzer.lucene;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * Created by malteschwarzer on 30.11.14.
 */
public class HDFSReader {
    String hdfsPath;
    FileSystem fileSystem;

    public HDFSReader(String hdfsPath) {
        this.hdfsPath = hdfsPath;

    }
    public BufferedReader getReader() throws IOException, InterruptedException {
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://ibm-power-1.dima.tu-berlin.de:8020/");
        conf.set("fs.default.name", "hdfs://ibm-power-1.dima.tu-berlin.de:8020/");

        conf.set("io.file.buffer.size", "131072");
        conf.set("hadoop.http.staticuser.user", "hadoop");
        conf.set("dfs.namenode.hosts", "ibm-power-1.dima.tu-berlin.de");
        conf.set("dfs.namenode.name.dir", "file:///hadoop/hdfs/hadoop/namenode");
        conf.set("dfs.datanode.data.dir", "file:///hadoop/hdfs/data/1/hadoop/datanode,file:///hadoop/hdfs/data/2/hadoop/datanode,file:///hadoop/hdfs/data/3/hadoop/datanode,file:///hadoop/hdfs/data/4/hadoop/datanode");
        conf.set("dfs.blocksize", "128m");

        // Init HDFS
        URI uri = URI.create(hdfsPath);
        fileSystem = FileSystem.get(uri, conf, "hadoop");
        FSDataInputStream in = fileSystem.open(new Path(uri));

        return new BufferedReader(new InputStreamReader(in));
    }

    public void close() throws IOException {
        fileSystem.close();
    }
}
