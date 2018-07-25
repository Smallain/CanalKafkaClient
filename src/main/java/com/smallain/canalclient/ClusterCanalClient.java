package com.smallain.canalclient;


import com.smallain.hdfsclient.HdfsDao;
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


        //命令行参数列表

        String zookeeperPath = parakvMap.get("--zookeeper").toString();
        String kfkServers = parakvMap.get("--kafka-bootstrap-servers").toString();
        String clientId = parakvMap.get("--kafka-clientid").toString();
        String topicKfk = parakvMap.get("--kfktopic").toString();
        String hadoopUrl = parakvMap.get("--hdfs-url").toString();
        String canalSourceFilePath = parakvMap.get("--canal-source-path").toString();
        String canalCityFilePath = parakvMap.get("--canal-city-path").toString();
        String canalPartitionFilePath = parakvMap.get("--canal-partition-path").toString();

        // 基于固定canal server的地址，建立链接，其中一台server发生crash，可以支持failover
        // CanalConnector connector = CanalConnectors.newClusterConnector(
        // Arrays.asList(new InetSocketAddress(
        // AddressUtils.getHostIp(),
        // 11111)),
        // "stability_test", "", "");

        //可配置订阅多个destinationList
        ArrayList canalSources = null;
        HdfsDao canalSourceRead = new HdfsDao();
        try {
            canalSources = canalSourceRead.readCanalSouces(hadoopUrl, canalSourceFilePath);
        } catch (Exception e) {
            e.printStackTrace();
        }


        // 基于固定canal server的地址，建立链接，其中一台server发生crash，可以支持failover
        // CanalConnector connector = CanalConnectors.newClusterConnector(
        // Arrays.asList(new InetSocketAddress(
        // AddressUtils.getHostIp(),
        // 11111)),
        // "stability_test", "", "");


        //可配置订阅多个destinationList
        //String[] destinationList = {"canal_bn_bj", "canal_bn_sh"};

        //循环创建引用canal server的对象实例
        /**
         * 基于canal配置文件中数据源数量开启进程处理。
         * 此处需要注意，如果两个 destination 的配置信息属于一个 数据节点，那么数据可能会存在重复消费，因为即使不同的数据库示例还是属于同一个数据库，
         * 这方面可以配置canal sercer的解析策略去实现。
         */
        for (int i = 0; i < canalSources.size(); i++) {
            // 基于zookeeper动态获取canal server的地址，建立链接，其中一台server发生crash，可以支持failover
            CanalConnector connector = CanalConnectors.newClusterConnector(zookeeperPath, canalSources.get(i).toString(), "", "");
            final ClusterCanalClient clientTest = new ClusterCanalClient(canalSources.get(i).toString());
            clientTest.setConnector(connector);
            clientTest.start(kfkServers, clientId, topicKfk, canalSources.get(i).toString(), hadoopUrl, canalCityFilePath, canalPartitionFilePath);
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
