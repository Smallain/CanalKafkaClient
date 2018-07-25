package com.smallain.hdfsclient;

import java.util.ArrayList;
import java.util.Map;

public class TestHdfs {
    public static void main(String[] args) {
        HdfsDao hd = new HdfsDao();
        HdfsDao hd2 = new HdfsDao();
        ArrayList al;
//        try {
//            al = hd.readCanalSouces("hdfs://10.200.48.67:8020", "/user/wuyuhang/canal_source_city.txt");
//            for (int i = 0; i < al.size(); i++) {
//                System.out.println(al.get(i).toString());
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        try {
            Map tempMap = hd2.readCanalCity("hdfs://10.200.48.67:8020", "/user/wuyuhang/canal_source_city.txt");
            System.out.println(tempMap.toString());
            String beijing = tempMap.get("canal_bn_bj").toString();
            System.out.println(beijing);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
