package de.tuberlin.dima.mschwarzer.lucene;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;

public class HDFSReader {
    /**
     * Logger for this class.
     */
    private static Logger LOG = Logger.getLogger(HDFSReader.class);

    String hdfsPath;
    FileSystem fileSystem;
    private boolean localFSReader = false;

    public HDFSReader(String hdfsPath) {
        this.hdfsPath = hdfsPath;

        if(!hdfsPath.substring(0, 8).equals("hdfs://")) {
            System.out.println("Use local reader");
            localFSReader = true;
        }

    }
    public BufferedReader getReader() throws IOException, InterruptedException {
        if(localFSReader) {
            return new BufferedReader(new FileReader(new File(hdfsPath)));
        } else {
            Configuration conf = new Configuration();

            conf.set("fs.defaultFS", "hdfs://ibm-power-1.dima.tu-berlin.de:8020/");
            conf.set("fs.default.name", "hdfs://ibm-power-1.dima.tu-berlin.de:8020/");

            conf.set("io.file.buffer.size", "131072");
            conf.set("hadoop.http.staticuser.user", "hadoop");
            conf.set("dfs.namenode.hosts", "ibm-power-1.dima.tu-berlin.de");
            conf.set("dfs.namenode.name.dir", "file:///hadoop/hdfs/hadoop/namenode");
            conf.set("dfs.datanode.data.dir", "file:///hadoop/hdfs/data/1/hadoop/datanode,file:///hadoop/hdfs/data/2/hadoop/datanode,file:///hadoop/hdfs/data/3/hadoop/datanode,file:///hadoop/hdfs/data/4/hadoop/datanode");
            conf.set("dfs.blocksize", "128m");

            LOG.debug("Connecting to HDFS");

            // Init HDFS
            URI uri = URI.create(hdfsPath);
            fileSystem = FileSystem.get(uri, conf, "hadoop");
            FSDataInputStream in = fileSystem.open(new Path(uri));

            return new BufferedReader(new InputStreamReader(in));
        }
    }

    public void close() throws IOException {
        if(!localFSReader) {
            fileSystem.close();

            LOG.debug("HDFS connection closed");
        }
    }
}
