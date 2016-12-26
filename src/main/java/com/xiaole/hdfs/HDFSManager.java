package com.xiaole.hdfs;

import com.xiaole.mvc.model.LogChecker;
import com.xiaole.mvc.model.LogFilter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import com.xiaole.utils.DownloadFileUtil;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by llc on 16/11/16.
 */

public class HDFSManager {
    private static final Logger logger = Logger.getLogger(HDFSManager.class);
    // initialization
    static Configuration conf = new Configuration();
    static FileSystem hdfs;
//     private static final String logAddress = "101.201.82.247"; // 公网ip
    private static final String logAddress = "10.252.0.171";    // 内网ip

    private static final String hadoopUser = "hadoop";

    private static final String hdfsAddress = "hdfs://" + logAddress + ":19000/";

    // 日志检查器
    private static LogChecker logChecker = new LogChecker();

    static {
        UserGroupInformation ugi = UserGroupInformation
                .createRemoteUser(hadoopUser);
        try {
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception {
                    Configuration conf = new Configuration();
                    conf.set("fs.defaultFS", hdfsAddress);
                    conf.set("hadoop.job.ugi", hadoopUser);
                    Path path = new Path(hdfsAddress);
                    hdfs = FileSystem.get(path.toUri(), conf);
                    //hdfs = path.getFileSystem(conf); // 这个也可以
                    //hdfs = FileSystem.get(conf); //这个不行，这样得到的hdfs所有操作都是针对本地文件系统，而不是针对hdfs的，原因不太清楚
                    return null;
                }
            });
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    // create a direction
    public void createDir(String dir) throws IOException {
        Path path = new Path(dir);
        if (hdfs.exists(path)) {
            System.out.println("dir \t" + conf.get("fs.default.name") + dir
                    + "\t already exists");
            return;
        }
        hdfs.mkdirs(path);
        System.out.println("new dir \t" + conf.get("fs.default.name") + dir);
    }
    // copy from local file to HDFS file
    public void copyFile(String localSrc, String hdfsDst) throws IOException {
        Path src = new Path(localSrc);
        Path dst = new Path(hdfsDst);
        if (!(new File(localSrc)).exists()) {
            System.out.println("Error: local dir \t" + localSrc
                    + "\t not exists.");
            return;
        }
        if (!hdfs.exists(dst)) {
            System.out.println("Error: dest dir \t" + dst.toUri()
                    + "\t not exists.");
            return;
        }
        String dstPath = dst.toUri() + "/" + src.getName();
        if (hdfs.exists(new Path(dstPath))) {
            System.out.println("Warn: dest file \t" + dstPath
                    + "\t already exists.");
        }
        hdfs.copyFromLocalFile(src, dst);
        // list all the files in the current direction
        FileStatus files
                [] = hdfs.listStatus(dst);
        System.out.println("Upload to \t" + conf.get("fs.default.name")
                + hdfsDst);
        for (FileStatus file : files) {
            System.out.println(file.getPath());
        }
    }

    // create a new file
    public void createFile(String fileName, String fileContent)
            throws IOException {
        Path dst = new Path(fileName);
        byte[] bytes = fileContent.getBytes();
        FSDataOutputStream output = hdfs.create(dst);
        output.write(bytes);
        System.out.println("new file \t" + conf.get("fs.default.name")
                + fileName);
    }

    // create a new file
    public void appendFile(String fileName, String fileContent)
            throws IOException {
        Path dst = new Path(fileName);
        byte[] bytes = fileContent.getBytes();
        if (!hdfs.exists(dst)) {
            createFile(fileName, fileContent);
            return;
        }
        FSDataOutputStream output = hdfs.append(dst);
        output.write(bytes);
        System.out.println("append to file \t" + conf.get("fs.default.name")
                + fileName);
    }

    // list all files
    public void listFiles(String dirName) throws IOException {
        Path f = new Path(dirName);
        FileStatus[] status = hdfs.listStatus(f);
        System.out.println(dirName + " has all files:");
        for (int i = 0; i < status.length; i++) {
            System.out.println(status[i].getPath().toString());
        }
    }

    // judge a file existed? and delete it!
    public void deleteFile(String fileName) throws IOException {
        Path f = new Path(fileName);
        boolean isExists = hdfs.exists(f);
        if (isExists) { // if exists, delete
            boolean isDel = hdfs.delete(f, true);
            System.out.println(fileName + "  delete? \t" + isDel);
        } else {
            System.out.println(fileName + "  exist? \t" + isExists);
        }
    }

    // read a file if existed
    public void readFile(String fileName) throws IOException {
        try{
            FSDataInputStream inStream = hdfs.open(new Path(fileName));
            BufferedReader bReader = new BufferedReader(new InputStreamReader(inStream));
            String line = "";
            while ((line = bReader.readLine()) != null){
                System.out.println(line);
            }
        } catch (Exception e){
            e.printStackTrace();
        }

    }

    // readLines method given a file
    public List<String> readLines(String fileName, String module, String memberId) throws IOException{
        FSDataInputStream inStream = hdfs.open(new Path(fileName));
        BufferedReader bReader = new BufferedReader(new InputStreamReader(inStream));
        String line = "";

        LogFilter logFilter = new LogFilter(memberId, module);
        List<String> logList = new ArrayList<String>();

        if (logFilter.isHasFilter()){
            while ((line = bReader.readLine()) != null){
                if (logFilter.filter(line) && logChecker.check(line)) {
                    logList.add(line);
                }
            }
        }
        else{
            while ((line = bReader.readLine()) != null){
                if (logChecker.check(line))
                    logList.add(line);
            }
        }
        return logList;
    }

    public List<String> getLogByDate(String date, String module, String memberId){
        String fileName = "/data/logstash/stats/" + date + ".log";
        List<String> logList = new ArrayList<String>();
        try {
            logList = readLines(fileName, module, memberId);
        } catch (Exception e) {
            logger.error("error occurs: " + e.getMessage());
        }
        return logList;
    }

    public static void main(String[] args) throws IOException {
        HDFSManager ofs = new HDFSManager();
        ofs.readFile("/data/txt.log");
        System.out.println("\n=======create dir=======");
        String dir = "/test12";
        ofs.createDir(dir);
        System.out.println("\n=======copy file=======");
        String src = "/Users/linchuan/llc.txt";
        ofs.copyFile(src, dir);
        System.out.println("\n=======create a file=======");
        String fileContent = "Hello, world! Just a test.";
        ofs.appendFile(dir+"/word.txt", fileContent);
    }

    public void downloadLogByDate(String fileName, String saveName, HttpServletResponse response) throws IOException{
//        String fileName = "/input/core-site.xml";
        FSDataInputStream inStream = hdfs.open(new Path(fileName));
        DownloadFileUtil.pushLinesFile(saveName, inStream, response);
    }

    public void downloadLogByList(List<String> logList, String saveName, HttpServletResponse response) throws IOException{
        String path = "/root/log4j/hxx/logService/download/" + saveName + ".log";
        File tmpFile = new File(path);
        FileUtils.writeLines(tmpFile, logList);
        DownloadFileUtil.pushFile(saveName, path, response);
        FileUtils.forceDelete(tmpFile);
    }

}

