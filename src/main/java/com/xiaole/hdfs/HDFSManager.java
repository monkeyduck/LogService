package com.xiaole.hdfs;

import com.xiaole.mvc.model.LogFilter;
import com.xiaole.mvc.model.NormalLog;
import com.xiaole.mvc.model.checker.InvalidLogChecker;
import com.xiaole.mvc.model.checker.SdkLogChecker;
import com.xiaole.mvc.model.checker.simpleChecker.CheckSimpleInterface;
import com.xiaole.mvc.model.checker.simpleChecker.SimpleCheckerFactory;
import com.xiaole.mvc.model.comparator.LogComparator;
import com.xiaole.mvc.model.comparator.MyComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URLEncoder;
import java.security.PrivilegedExceptionAction;
import java.util.*;

/**
 * Created by llc on 16/11/16.
 */

public class HDFSManager {
    private static final Logger logger = Logger.getLogger(HDFSManager.class);
    // initialization
    static Configuration conf = new Configuration();
    static FileSystem hdfs;
//     private static final String logAddress = "101.201.103.114"; // 公网ip
    private static final String logAddress = "10.252.0.171";    // 内网ip

    private static final String hadoopUser = "hadoop";

    private static final String hdfsAddress = "hdfs://" + logAddress + ":19000/";

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
    public List<String> readLines(String fileName, String module, String memberId, String env, String level)
            throws IOException{
        FSDataInputStream inStream = hdfs.open(new Path(fileName));
        BufferedReader bReader = new BufferedReader(new InputStreamReader(inStream));
        String line = "";

        LogFilter logFilter = new LogFilter(memberId, module, env, level);
        List<String> logList = new ArrayList<String>();

        if (logFilter.isHasFilter()){
            while ((line = bReader.readLine()) != null){
                if (logFilter.filter(line) && InvalidLogChecker.check(line)) {
                    logList.add(line);
                }
            }
        }
        else{
            while ((line = bReader.readLine()) != null){
                if (InvalidLogChecker.check(line))
                    logList.add(line);
            }
        }
        return logList;
    }

    public List<String> readBevaLines(String fileName, String module, String memberId, String env, String level) throws IOException {
        FSDataInputStream inStream = hdfs.open(new Path(fileName));
        BufferedReader bReader = new BufferedReader(new InputStreamReader(inStream));
        String line = "";

        LogFilter logFilter = new LogFilter(memberId, module, env, level);
        List<String> logList = new ArrayList<String>();

        if (logFilter.isHasFilter()){
            while ((line = bReader.readLine()) != null){
                if (logFilter.filter(line) && InvalidLogChecker.check(line) && SdkLogChecker.check(line)) {
                    logList.add(line);
                }
            }
        }
        else{
            while ((line = bReader.readLine()) != null){
                if (InvalidLogChecker.check(line) && SdkLogChecker.check(line))
                    logList.add(line);
            }
        }
        return logList;
    }

    // Read simple lines from given file
    public List<String> readSimpleLines(String fileName, String module, String memberId, String env, String level,
                                        String src)
            throws IOException{
        FSDataInputStream inStream = hdfs.open(new Path(fileName));
        BufferedReader bReader = new BufferedReader(new InputStreamReader(inStream));
        String line = "";

        LogFilter logFilter = new LogFilter(memberId, module, env, level);
        List<String> logList = new ArrayList<String>();
        int queueSize = 100;
        Queue<NormalLog> pq = new PriorityQueue<>(queueSize, new LogComparator());
        NormalLog log;
        CheckSimpleInterface simpleChecker = SimpleCheckerFactory.genSimpleChecker(src);
        if (logFilter.isHasFilter()){
            while ((line = bReader.readLine()) != null){
                try {
                    log = new NormalLog(line);
                }catch (Exception e){
                    logger.error("Parse log error: " + e.getMessage() + "log:" + line);
                    continue;
                }
                if (logFilter.filter(log) && simpleChecker.checkSimple(line)) {
                    pq.add(log);
                    if (pq.size() == queueSize) {
                        logList.addAll(pq.poll().toNewSimpleFormat());
                    }
                }
            }
        }
        else{
            while ((line = bReader.readLine()) != null){
                if (simpleChecker.checkSimple(line)) {
                    try {
                        log = new NormalLog(line);
                    }catch (Exception e){
                        logger.error("Parse log error: " + e.getMessage() + "log:" + line);
                        continue;
                    }
                    pq.add(log);
                    if (pq.size() == queueSize) {
                        logList.addAll(pq.poll().toNewSimpleFormat());
                    }
                }
            }
        }
        // 将队列中剩余的日志按顺序加入logList
        for (NormalLog leftLog : pq) {
            logList.addAll(leftLog.toNewSimpleFormat());
        }
        return logList;
    }

    // read all users simple logs from given file
    public Map<String, List<String> > readAllUserSimpleLines(String fileName, String module, String env, String level,
                                                             String src)
            throws IOException {
        FSDataInputStream inStream = hdfs.open(new Path(fileName));
        BufferedReader bReader = new BufferedReader(new InputStreamReader(inStream));
        String line = "";
        LogFilter logFilter = new LogFilter("all", module, env, level);
        int queueSize = 100;
        Queue<NormalLog> pq = new PriorityQueue<>(queueSize, new LogComparator());
        NormalLog log;
        Map<String, List<String> > map = new HashMap<>();
        CheckSimpleInterface simpleChecker = SimpleCheckerFactory.genSimpleChecker(src);
        if (logFilter.isHasFilter()){
            while ((line = bReader.readLine()) != null){
                try {
                    log = new NormalLog(line);
                }catch (Exception e){
                    logger.error("Parse log error: " + e.getMessage() + "log:" + line);
                    continue;
                }
                if (logFilter.filter(log) && simpleChecker.checkSimple(line)) {
                    pq.add(log);
                    if (pq.size() == queueSize) {
                        NormalLog topLog = pq.poll();
                        if (!map.containsKey(topLog.getMember_id())) {
                            logger.info("map add new memberId: " + topLog.getMember_id());
                            map.put(topLog.getMember_id(), new ArrayList<>());
                        }
                        map.get(topLog.getMember_id()).addAll(topLog.toNewSimpleFormat());
                    }
                }
            }
        }
        else{
            while ((line = bReader.readLine()) != null){
                if (simpleChecker.checkSimple(line)) {
                    try {
                        log = new NormalLog(line);
                    }catch (Exception e){
                        logger.error("Parse log error: " + e.getMessage() + "log:" + line);
                        continue;
                    }
                    pq.add(log);
                    if (pq.size() == queueSize) {
                        NormalLog topLog = pq.poll();
                        if (!map.containsKey(topLog.getMember_id())) {
                            map.put(topLog.getMember_id(), new ArrayList<>());
                        }
                        map.get(topLog.getMember_id()).addAll(topLog.toNewSimpleFormat());
                    }
                }
            }
        }
        // 将队列中剩余的日志按顺序加入logList
        for (NormalLog leftLog : pq) {
            if (!map.containsKey(leftLog.getMember_id())) {
                map.put(leftLog.getMember_id(), new ArrayList<>());
            }
            map.get(leftLog.getMember_id()).addAll(leftLog.toNewSimpleFormat());
        }
        return map;
    }

    public List<String> getLogByDate(String date, String module, String memberId, String env, String level, String src){
        String fileName = "/data/logstash/stats/" + date + ".log";
        List<String> logList = new ArrayList<String>();
        try {
            if (src.equals("xiaole")) {
                logList = readLines(fileName, module, memberId, env, level);
            } else {
                logList = readBevaLines(fileName, module, memberId, env, level);
            }
        } catch (Exception e) {
            logger.error("error occurs: " + e.getMessage());
        }
        return logList;
    }

    public List<String> getSimpleLogByDate(String date, String module, String memberId, String env, String level,
                                           String src){
        String fileName = "/data/logstash/stats/" + date + ".log";
        List<String> logList = new ArrayList<String>();
        try {
            logList = readSimpleLines(fileName, module, memberId, env, level, src);
        } catch (Exception e) {
            logger.error("error occurs: " + e.getMessage());
        }
        return logList;
    }

    public Map<String, List<String> > getAllUserSimpleLog(String date, String module, String env, String level,
                                                          String src) {
        String fileName = "/data/logstash/stats/" + date + ".log";
        Map<String, List<String> > map = new HashMap<>();
        try {
            map = readAllUserSimpleLines(fileName, module, env, level, src);
        } catch (Exception e) {
            logger.error("error occurs: " + e.getMessage());
        }
        return map;
    }

    public void downloadLog(String date, String saveName, HttpServletResponse response) throws IOException {
        String fileName = "/data/logstash/stats/" + date + ".log";
        FSDataInputStream in = hdfs.open(new Path(fileName));
        response.setHeader("content-disposition", "attachment;filename=" + URLEncoder.encode(saveName, "UTF-8"));
        OutputStream out = response.getOutputStream();
        IOUtils.copyBytes(in, out, 4096, true);
    }

    public static void main(String[] args) {
        Queue<Integer> pq = new java.util.PriorityQueue<>(4, new MyComparator());
//        pq.add(3);
//        pq.add(6);
//        pq.add(1);
//        pq.add(0);
//        pq.add(5);
//        for (int i : pq) {
//            System.out.println(i);
//        }
        System.out.println("123".compareTo("124"));
    }

}

