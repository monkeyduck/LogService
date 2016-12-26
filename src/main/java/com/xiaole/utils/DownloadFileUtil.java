package com.xiaole.utils;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataInputStream;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URLEncoder;

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
}
