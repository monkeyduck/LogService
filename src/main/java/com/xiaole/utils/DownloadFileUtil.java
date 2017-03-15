package com.xiaole.utils;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataInputStream;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

/**
 * Created by hxx on 5/27/16.
 */
public class DownloadFileUtil {

    public static void pushFile(String saveName, String path, HttpServletResponse response) throws IOException {
        response.setHeader("content-disposition", "attachment;filename=" + URLEncoder.encode(saveName, "UTF-8"));
        OutputStream out = response.getOutputStream();
        File file = new File(path);
        FileUtils.copyFile(file, out);
        out.close();
    }

    public static void pushLinesFile(String saveName, FSDataInputStream inStream, HttpServletResponse response) throws IOException{
        response.setCharacterEncoding("utf-8");
        response.setContentType("multipart/form-data");
        response.setHeader("Content-Disposition", "attachment;fileName="
                + saveName);
//        response.setHeader("content-disposition", "attachment;filename=" + URLEncoder.encode(saveName, "UTF-8"));
        OutputStream out = response.getOutputStream();
        byte[] b = new byte[2048];
        int length;
        while ((length = inStream.read(b)) > 0) {
            out.write(b, 0, length);
        }
        out.close();
    }

    public static void downloadLogByList(List<String> logList, String saveName, HttpServletResponse response)
            throws IOException{
        String path = "/var/log/hdfsDownload/" + saveName;
        File tmpFile = new File(path);
        FileUtils.writeLines(tmpFile, logList);
        DownloadFileUtil.pushFile(saveName, path, response);
        FileUtils.forceDelete(tmpFile);
    }

    public static void downloadLogByMap(Map<String, List<String> > logMap, String saveName, HttpServletResponse response)
            throws IOException {
        String path = "/var/log/hdfsDownload/" + saveName;
        File tmpFile = new File(path);
        for (Map.Entry<String, List<String> > entry: logMap.entrySet()) {
            FileUtils.writeLines(tmpFile, entry.getValue(), true);
            FileUtils.write(tmpFile, "\r\n-------------------------我是分割线-------------------------\r\n", true);
        }
        DownloadFileUtil.pushFile(saveName, path, response);
        FileUtils.forceDelete(tmpFile);
    }
}
