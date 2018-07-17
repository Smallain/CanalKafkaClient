package com.smallain.canalclient;


import org.apache.commons.lang.exception.ExceptionUtils;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.smallain.util.ArgsParse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.smallain.util.ArgsParse.getArgsMap;

/**
 * 集群模式的测试例子
 *
 * @author jianghang 2013-4-15 下午04:19:20
 * @version 1.0.4
 */
public class ClusterCanalClient extends AbstractCanalClient {

    public ClusterCanalClient(String destination) {
        super(destination);
    }


    public static void main(String args[]) {


        List paras = Arrays.asList(args);
        ArrayList temp = new ArrayList(paras);
        Map parakvMap = getArgsMap(temp);

        System.out.println(parakvMap.toString());

        String zookeeperPath = parakvMap.get("--zookeeper").toString();
        String kfkServers = parakvMap.get("--kafka-bootstrap-servers").toString();
        String clientId = parakvMap.get("--kafka-clientid").toString();
        String topicKfk = parakvMap.get("--kfktopic").toString();


        // 基于固定canal server的地址，建立链接，其中一台server发生crash，可以支持failover
        // CanalConnector connector = CanalConnectors.newClusterConnector(
        // Arrays.asList(new InetSocketAddress(
        // AddressUtils.getHostIp(),
        // 11111)),
        // "stability_test", "", "");


        //可配置订阅多个destinationList
        String[] destinationList = {"canal_bn_bj"};

        //循环创建引用canal server的对象实例
        for (int i = 0; i < destinationList.length; i++) {
            // 基于zookeeper动态获取canal server的地址，建立链接，其中一台server发生crash，可以支持failover
            CanalConnector connector = CanalConnectors.newClusterConnector(zookeeperPath, destinationList[i], "", "");
            final ClusterCanalClient clientTest = new ClusterCanalClient(destinationList[i]);
            clientTest.setConnector(connector);
            clientTest.start(kfkServers, clientId, topicKfk);
            Runtime.getRuntime().addShutdownHook(new Thread() {

                public void run() {
                    try {
                        logger.info("## stop the canal client");
                        clientTest.stop();
                    } catch (Throwable e) {
                        logger.warn("##something goes wrong when stopping canal:\n{}", ExceptionUtils.getFullStackTrace(e));
                    } finally {
                        logger.info("## canal client is down.");
                    }

                }
            });

        }

    }
}
