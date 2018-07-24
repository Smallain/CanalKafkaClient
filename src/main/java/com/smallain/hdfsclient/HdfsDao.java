package com.smallain.hdfsclient;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

public class HdfsDao {
    FileSystem fs;


    /**
     * 获取canal数据源信息配置文件，存于hdfs上，每次启动去读取配置文件
     *
     * @param hdfsUrl   hdfs的url信息
     * @param canalSourceFilePath canal destination 源信息
     * @return 返回canal 数据源 列表 如 {canal_bn_bj,canal_bn_sh}
     * @throws Exception
     */
    public ArrayList readCanalSouces(String hdfsUrl, String canalSourceFilePath) throws Exception {
        URI uri = new URI(hdfsUrl);
        Configuration conf = new Configuration();
        fs = FileSystem.get(uri, conf);
        FSDataInputStream fin = fs.open(new Path(canalSourceFilePath));
        BufferedReader in = null;
        String line;
        ArrayList canalList = new ArrayList();
        try {
            in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
            while ((line = in.readLine()) != null) {

                canalList.add(line);
            }
        } finally {
            if (in != null) {
                in.close();
            }
        }

        return canalList;

    }
}
