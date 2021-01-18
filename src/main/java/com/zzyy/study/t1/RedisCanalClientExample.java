package com.zzyy.study.t1;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;
import com.zzyy.study.util.RedisUtils;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import redis.clients.jedis.Jedis;

/**
 * @auther zzyy
 * @create 2020-11-11 17:13
 */
public class RedisCanalClientExample
{

    public static final Integer _60SECONDS = 60;

    public static void main(String args[]) {

        // 创建链接canal服务端
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("127.0.0.1",
                11111), "example", "", "");
        int batchSize = 1000;
        int emptyCount = 0;
        System.out.println("----------------程序启动，开始监听mysql的变化：");
        try {
            connector.connect();
//            connector.subscribe(".*\\..*");
            connector.subscribe("groupon.t_user");
            connector.rollback();
            int totalEmptyCount = 10 * _60SECONDS;

            while (emptyCount < totalEmptyCount) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    emptyCount++;
                    try { TimeUnit.SECONDS.sleep(1); } catch (InterruptedException e) { e.printStackTrace(); }
                } else {
                    emptyCount = 0;
                    printEntry(message.getEntries());
                }
                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }
            System.out.println("empty too many times, exit");
        } finally {
            connector.disconnect();
        }
    }

    private static void printEntry(List<Entry> entrys) {
        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error,data:" + entry.toString(),e);
            }

            EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================ binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), eventType));

            for (RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == EventType.INSERT) {
                    redisInsert(rowData.getAfterColumnsList());
                } else if (eventType == EventType.DELETE) {
                    redisDelete(rowData.getBeforeColumnsList());
                } else {//EventType.UPDATE
                    redisUpdate(rowData.getAfterColumnsList());
                }
            }
        }
    }

    private static void redisInsert(List<Column> columns)
    {
        JSONObject jsonObject = new JSONObject();
        for (Column column : columns)
        {
            System.out.println(column.getName() + " : " + column.getValue() + "    insert=" + column.getUpdated());
            jsonObject.put(column.getName(),column.getValue());
        }
        if(columns.size() > 0)
        {
            try(Jedis jedis = RedisUtils.getJedis())
            {
                jedis.set(columns.get(0).getValue(),jsonObject.toJSONString());
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    private static void redisDelete(List<Column> columns)
    {
        JSONObject jsonObject = new JSONObject();
        for (Column column : columns)
        {
            System.out.println(column.getName() + " : " + column.getValue() + "    delete=" + column.getUpdated());
            jsonObject.put(column.getName(),column.getValue());
        }
        if(columns.size() > 0)
        {
            try(Jedis jedis = RedisUtils.getJedis())
            {
                jedis.del(columns.get(0).getValue());
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    private static void redisUpdate(List<Column> columns)
    {
        JSONObject jsonObject = new JSONObject();
        for (Column column : columns)
        {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
            jsonObject.put(column.getName(),column.getValue());
        }
        if(columns.size() > 0)
        {
            try(Jedis jedis = RedisUtils.getJedis())
            {
                jedis.set(columns.get(0).getValue(),jsonObject.toJSONString());
                System.out.println("---------update after: "+jedis.get(columns.get(0).getValue()));
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        long startTime = System.currentTimeMillis();

        long endTime = System.currentTimeMillis();
        System.out.println("----costTime: "+(endTime - startTime) +" 毫秒");

    }
    

    //===========================================================================================
    //  下面都是伪代码，参考思路，
    /*public void deleteOrderData(Order order)
    {
        try(Jedis jedis = RedisUtils.getJedis())
        {
            //1 线程A先成功删除redis缓存
            jedis.del(order.getId()+"");
            //2 线程A再更新mysql
            orderDao.update(order);
            //暂停20秒钟，其它业务逻辑导致耗时延时，20是随便乱写的，只是为了讲解技术方便
            try { TimeUnit.SECONDS.sleep(20); } catch (InterruptedException e) { e.printStackTrace(); }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public Order selectOrderData(Order order)
    {
        try(Jedis jedis = RedisUtils.getJedis())
        {
            //1 先去redis里面查找,找到返回数据找不到去mysql查找
            String result = jedis.get(order.getId() + "");
            if (result != null) {
                return (Order) JSON.parse(result);
            }else{
                order = orderDao.getOrderById(order.getId());
                //2 线程B会将从mysql查到的旧数据写回到redis
                jedis.set(order.getId()+"",order.toString());
                return order;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public void deleteDoubleOrderDelay2(Order order)
    {
        try(Jedis jedis = RedisUtils.getJedis())
        {
            //1 线程A先成功删除redis缓存
            jedis.del(order.getId()+"");
            //2 线程A再更新mysql
            orderDao.update(order);
            //暂停20秒钟
            try { TimeUnit.SECONDS.sleep(20); } catch (InterruptedException e) { e.printStackTrace(); }
            CompletableFuture.supplyAsync(() -> {
                //3 将第二次删除作为异步的。自己起一个线程，异步删除。
                // 这样，写的请求就不用沉睡一段时间后了，再返回。这么做，加大吞吐量。
                return jedis.del(order.getId() + "");
            }).whenComplete((t,u) -> {
                System.out.println("------t:"+t);
                System.out.println("------t:"+u);
            }).exceptionally(e ->{
                System.out.println("------e: "+e.getMessage());
                return 44L;
            }).get();
        }catch (Exception e){
            e.printStackTrace();
        }
    }*/



}
