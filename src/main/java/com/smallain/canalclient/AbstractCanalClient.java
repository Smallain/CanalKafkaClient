package com.smallain.canalclient;


import java.text.SimpleDateFormat;
import java.util.*;

import com.alibaba.fastjson.JSON;
import com.smallain.canalclient.model.DataBaseModel;
import com.smallain.kafkaclient.KafKaProducerFactory;
import org.apache.commons.collections.bidimap.TreeBidiMap;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.CanalEntry.TransactionBegin;
import com.alibaba.otter.canal.protocol.CanalEntry.TransactionEnd;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * 测试基类
 *
 * @author jianghang 2013-4-15 下午04:17:12
 * @version 1.0.4
 */
public class AbstractCanalClient {

    protected final static Logger logger = LoggerFactory.getLogger(AbstractCanalClient.class);
    protected static final String SEP = SystemUtils.LINE_SEPARATOR;
    protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    protected volatile boolean running = false;
    protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {

        public void uncaughtException(Thread t, Throwable e) {
            logger.error("parse events has an error", e);
        }
    };


    protected Thread thread = null;
    protected CanalConnector connector;
    protected static String context_format = null;
    protected static String row_format = null;
    protected static String transaction_format = null;
    protected String destination;

    static {
        context_format = SEP + "****************************************************" + SEP;
        context_format += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP;
        context_format += "* Start : [{}] " + SEP;
        context_format += "* End : [{}] " + SEP;
        context_format += "****************************************************" + SEP;

        row_format = SEP
                + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {} , delay : {}ms"
                + SEP;

        transaction_format = SEP + "================> binlog[{}:{}] , executeTime : {} , delay : {}ms" + SEP;

    }


    public AbstractCanalClient(String destination) {
        this(destination, null);
    }

    public AbstractCanalClient(String destination, CanalConnector connector) {
        this.destination = destination;
        this.connector = connector;
    }

    protected void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(new Runnable() {

            public void run() {
                process();
            }
        });

        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running = true;
    }

    protected void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }

        MDC.remove("destination");
    }

    protected void process() {
        String database = "";
        String table = "";
        String type = "";
        String ts = "";
        List data = new ArrayList();


        int batchSize = 5 * 1024;
        while (running) {
            try {
                MDC.put("destination", destination);
                connector.connect();
                connector.subscribe();
                while (running) {
                    Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        // try {
                        // Thread.sleep(1000);
                        // } catch (InterruptedException e) {
                        // }
                    } else {

                        //具体处理数据逻辑
                        printSummary(message, batchId, size);
                        List db_info_list = printEntry(message.getEntries());
                        for (int i = 0; i < db_info_list.size(); i++) {
                            if (i == 0) {
                                database = db_info_list.get(i).toString();
                            } else if (i == 1) {
                                table = db_info_list.get(i).toString();
                            } else if (i == 2) {
                                type = db_info_list.get(i).toString().toLowerCase();
                            } else if (i == 3) {
                                ts = db_info_list.get(i).toString();
                            } else if (i == 4) {
                                data = ((List) db_info_list.get(i));
                            }

                        }

                        DataBaseModel dbm = new DataBaseModel();
                        dbm.database = database;
                        dbm.table = table;
                        dbm.type = type;
                        dbm.ts = ts;
                        dbm.data = data;
                        System.out.println("最终转化的数据库是:" + dbm.database);

                        String json = JSON.toJSONString(dbm);

                        System.out.println("最终转化的json是：" + json);

                        String kfkServers = "iz2zea86z2leonw09hpjijz:9092,iz2zea86z2leonw09hpjimz:9092,iz2zea86z2leonw09hpjilz:9092,iz2zea86z2leonw09hpjikz:9092";
                        String clientId = "TestProducer";

                        KafKaProducerFactory kafkaproducers = new KafKaProducerFactory(kfkServers, clientId);
                        kafkaproducers.pushKafKa("testkafka", 1, json);

                    }

                    connector.ack(batchId); // 提交确认
                    // connector.rollback(batchId); // 处理失败, 回滚数据
                }
            } catch (Exception e) {
                logger.error("process error!", e);
            } finally {
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }

    private void printSummary(Message message, long batchId, int size) {
        long memsize = 0;
        for (Entry entry : message.getEntries()) {
            memsize += entry.getHeader().getEventLength();
        }

        String startPosition = null;
        String endPosition = null;
        if (!CollectionUtils.isEmpty(message.getEntries())) {
            startPosition = buildPositionForDump(message.getEntries().get(0));
            endPosition = buildPositionForDump(message.getEntries().get(message.getEntries().size() - 1));
        }

        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        logger.info(context_format, new Object[]{batchId, size, memsize, format.format(new Date()), startPosition,
                endPosition});
    }

    protected String buildPositionForDump(Entry entry) {
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        return entry.getHeader().getLogfileName() + ":" + entry.getHeader().getLogfileOffset() + ":"
                + entry.getHeader().getExecuteTime() + "(" + format.format(date) + ")";
    }

    protected List printEntry(List<Entry> entrys) {

        List db_info = new ArrayList<>();
        String db_info_database;
        String db_info_table;
        String db_info_evenType;
        String db_info_executeTime;

        for (Entry entry : entrys) {
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;

            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN) {
                    TransactionBegin begin = null;
                    try {
                        begin = TransactionBegin.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务头信息，执行的线程id，事务耗时
                    logger.info(transaction_format,
                            new Object[]{entry.getHeader().getLogfileName(),
                                    String.valueOf(entry.getHeader().getLogfileOffset()),
                                    String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime)});
                    logger.info(" BEGIN ----> Thread id: {}", begin.getThreadId());
                } else if (entry.getEntryType() == EntryType.TRANSACTIONEND) {
                    TransactionEnd end = null;
                    try {
                        end = TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务提交信息，事务id
                    logger.info("----------------\n");
                    logger.info(" END ----> transaction id: {}", end.getTransactionId());
                    logger.info(transaction_format,
                            new Object[]{entry.getHeader().getLogfileName(),
                                    String.valueOf(entry.getHeader().getLogfileOffset()),
                                    String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime)});
                }

                continue;
            }

            if (entry.getEntryType() == EntryType.ROWDATA) {
                RowChange rowChage = null;
                try {
                    rowChage = RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }

                EventType eventType = rowChage.getEventType();

                logger.info(row_format,
                        new Object[]{entry.getHeader().getLogfileName(),
                                String.valueOf(entry.getHeader().getLogfileOffset()), entry.getHeader().getSchemaName(),
                                entry.getHeader().getTableName(), eventType,
                                String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime)});


                db_info_database = entry.getHeader().getSchemaName();
                db_info_table = entry.getHeader().getTableName();
                db_info_evenType = eventType.toString();
                db_info_executeTime = String.valueOf(entry.getHeader().getExecuteTime());

                db_info.add(db_info_database);
                db_info.add(db_info_table);
                db_info.add(db_info_evenType);
                db_info.add(db_info_executeTime);


                if (eventType == EventType.QUERY || rowChage.getIsDdl()) {
                    logger.info(" sql ----> " + rowChage.getSql() + SEP);
                    continue;
                }

                for (RowData rowData : rowChage.getRowDatasList()) {
                    if (eventType == EventType.DELETE) {
                        List dellist = printColumn(rowData.getBeforeColumnsList());
                        db_info.add(dellist);
                    } else if (eventType == EventType.INSERT) {
                        List inslist = printColumn(rowData.getAfterColumnsList());
                        db_info.add(inslist);
                    } else {
                        List update = printColumn(rowData.getAfterColumnsList());
                        db_info.add(update);
                    }
                }
            }
        }
        return db_info;
    }

    /**
     * 打印具体的数据库修改行信息
     *
     * @param columns
     */
    protected List printColumn(List<Column> columns) {
        List columnList = new ArrayList();
        for (Column column : columns) {
            StringBuilder builder = new StringBuilder();
            builder.append(column.getName() + " : " + column.getValue());
            builder.append("    type=" + column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=" + column.getUpdated());
            }

            columnList.add(column.getName());//列名
            columnList.add(column.getValue());//列值

            builder.append(SEP);
            logger.info(builder.toString());
        }

        return columnList;
    }

    public void setConnector(CanalConnector connector) {
        this.connector = connector;
    }

}
