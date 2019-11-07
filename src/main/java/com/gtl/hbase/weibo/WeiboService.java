package com.gtl.hbase.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * 1) 创建命名空间以及表名的定义
 * 2) 创建微博内容表
 * 3) 创建用户关系表
 * 4) 创建用户微博内容接收邮件表
 * 5) 发布微博内容
 * 6) 添加关注用户
 * 7) 移除（取关）用户
 * 8) 获取关注的人的微博内容
 * 9) 测试
 *
 * 用户微博内容接收邮件表--创建该表的原因：
 * 1、如果不创建该表，则需要先从t_user_relations表中查找当前用户关注的所有用户，然后再t_weibo表中查找所有被关注用户的最新微博，并且将这些微博按时间戳降序排序。
 *      存在两个问题：
 *      ①“最新”这个时间范围不明确，无法确定获取每个用户什么时间范围的微博(有的用户可能每天发很多微博，有的用户可能几周发一篇微博)
 *      ②由于userid是随机生成的，即不同的用户散列再各个分区，被关注的用户数据分布在不同的region，导致读取数据性能较差。
 * 2、创建了该表后，可以实时把每个被关注用户的最新微博新增到t_user_weibo_list表中，假定每个被关注用户可以保存100条微博，新微博会覆盖就微博，
 *    那么，既保证了最新微博的查询范围，也保证了每个用户的所有被关注用户的最新微博在同一个region(该表以userid为rowkey)。
 *
 */
public class WeiboService {

    private static Configuration conf = null;
    private static Connection conn = null;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 步骤：
     * 1、判断该命名空间是否存在
     * 2、不存在则创建该命名空间
     * @param ns
     */
    public static void createNamespace(byte[] ns){
        HBaseAdmin admin = null;
        try {
            admin = (HBaseAdmin) conn.getAdmin();

            // 1、判断该命名空间是否存在
            boolean exists = WeiboHbaseDaoImpl.namespaceExists(ns, admin);
            if (!exists) {
                // 2、如果不存在则创建该命名空间
                WeiboHbaseDaoImpl.createNamespace(ns,admin);
                System.out.println("创建namespace："+ Bytes.toString(ns)+"成功");
            }else {
                System.out.println("namespace："+ Bytes.toString(ns)+"已经存在");
            }
        } catch (IOException e) {
            System.out.println("创建namespace："+ Bytes.toString(ns)+"失败");
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 步骤：
     * 1、判断该表是否已经存在
     * 2、不存在则创建该表
     * @param ns
     * @param tn
     * @param cfs
     */
    public static void createTable(byte[] ns,byte[] tn,byte[][] cfs,int[] versions){
        HBaseAdmin admin = null;
        try {
            admin = (HBaseAdmin) conn.getAdmin();

            // 1、判断该表是否已经存在
            boolean exists = WeiboHbaseDaoImpl.tableExists(ns, tn, admin);
            if (exists) {
                System.out.println("表" + Bytes.toString(ns) + ":" + Bytes.toString(tn) + "已经存在");
            } else {
              WeiboHbaseDaoImpl.createTable(ns,tn,cfs,versions,admin);
                System.out.println("表" + Bytes.toString(ns) + ":" + Bytes.toString(tn) + "创建成功");
            }
        } catch (IOException e) {
            System.out.println("表" + Bytes.toString(ns) + ":" + Bytes.toString(tn) + "创建失败");
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


        // 2、不存在则创建该表
    }

    /**
     * 上传微博
     * 步骤：
     * 1、在t_weibo表中新增该条微博数据
     * 2、从t_user_relations表中查询该用户所有的粉丝用户
     * 3、遍历粉丝用户，在t_user_weibo_list表中，对每个粉丝用户新增接收微博数据
     * @param userid
     * @param title
     * @param content
     * @param image
     */
    public static void uploadWeibo(String userid,String title,String content,String image){

        HTable hTable_weibo = null;
        HTable hTable_user_relations = null;
        HTable table_user_weibo_list = null;
        try {
            // 1、在t_weibo表中新增该条微博数据
            hTable_weibo = (HTable) conn.getTable(TableName.valueOf(WeiboConstants.NS, WeiboConstants.TABLE_WEIBO));
            byte[] rowkey_weibo = createWeiboRowKey(userid);
            Put put_weibo = new Put(rowkey_weibo);
            put_weibo.addColumn(WeiboConstants.CF1,WeiboConstants.TITLE,title.getBytes());
            put_weibo.addColumn(WeiboConstants.CF1,WeiboConstants.CONTENT,content.getBytes());
            put_weibo.addColumn(WeiboConstants.CF1,WeiboConstants.IMAGE,image.getBytes());
            put_weibo.addColumn(WeiboConstants.CF1,WeiboConstants.USERID,userid.getBytes());
            List<Put> puts = new ArrayList<Put>();
            puts.add(put_weibo);
            WeiboHbaseDaoImpl.PutDatas(hTable_weibo, puts);
            System.out.println("用户" + userid + "发布微博成功");

            // 2、从t_user_relations表中查询该用户所有的粉丝用户
            List<byte[]> fans = new ArrayList<byte[]>();
            hTable_user_relations = (HTable) conn.getTable(TableName.valueOf(WeiboConstants.NS,WeiboConstants.TABLE_USER_RELATIONS));
            byte[] rowkey_relation = createUserRowKey(userid);
            Get get = new Get(rowkey_relation);
            get.addFamily(WeiboConstants.CF2);
            Result result_relation = WeiboHbaseDaoImpl.getOneRowData(hTable_user_relations, get);
            Cell[] cells = result_relation.rawCells();
            for (Cell cell : cells) {
                //获取粉丝用户列族下所有的列名，即为所有的粉丝userid
                byte[] row = CellUtil.cloneQualifier(cell);
                fans.add(row);
            }

            //3、遍历粉丝用户，在t_user_weibo_list表中，对每个粉丝用户新增接收微博数据
            if (!fans.isEmpty()) {
                table_user_weibo_list = (HTable) conn.getTable(TableName.valueOf(WeiboConstants.NS, WeiboConstants.TABLE_USER_WEIBO_LIST));
                List<Put> putList = new ArrayList<Put>();
                for (byte[] fan : fans) {
                    byte[] rowkey_user_weibo_list = createUserRowKey(Bytes.toString(fan));
                    Put put_user_weibo_list = new Put(rowkey_user_weibo_list);
                    put_user_weibo_list.addColumn(WeiboConstants.CF1,userid.getBytes(),put_weibo.getTimeStamp(),rowkey_weibo);
                    putList.add(put_user_weibo_list);
                }
                WeiboHbaseDaoImpl.PutDatas(table_user_weibo_list,putList);
            } else {
                System.out.println("用户" + userid + "没有粉丝，不需要同步微博到其粉丝的接收列表");
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (hTable_weibo != null) {
                try {
                    hTable_weibo.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (hTable_user_relations != null) {
                try {
                    hTable_user_relations.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (table_user_weibo_list != null) {
                try {
                    table_user_weibo_list.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 添加关注
     * 步骤：
     * 1、在t_user_relations表中，新增该用户的关注用户
     * 2、在t_user_relations表中，新增被关注用户的粉丝用户
     * 3、从t_weibo表中获取被关注用户最近的10条微博
     * 4、在t_user_weibo_list表中，给当前用户新增被关注用户的最近的10条微博
     * @param userid
     * @param focusUserId
     */
    public static void addFocus(String userid,String focusUserId){
        HTable table_relations = null;
        HTable table_weibo = null;
        HTable table_user_weibo_list = null;

        try {
            table_relations = (HTable) conn.getTable(TableName.valueOf(WeiboConstants.NS,WeiboConstants.TABLE_USER_RELATIONS));
            List<Put> puts = new ArrayList<Put>();

            // 1、在t_user_relations表中，新增该用户的关注用户
            Put put_focus = new Put(createUserRowKey(userid));
            put_focus.addColumn(WeiboConstants.CF1,focusUserId.getBytes(),WeiboConstants.DEFAULT_VALUE);
            puts.add(put_focus);

            //2、在t_user_relations表中，新增被关注用户的粉丝用户
            Put put_fans = new Put(createUserRowKey(focusUserId));
            put_fans.addColumn(WeiboConstants.CF2,userid.getBytes(),WeiboConstants.DEFAULT_VALUE);
            puts.add(put_fans);

            WeiboHbaseDaoImpl.PutDatas(table_relations,puts);
            System.out.println("用户" + userid + "的关注用户" +  focusUserId + "已添加");
            System.out.println("用户" + focusUserId + "的粉丝用户" +  userid + "已添加");

            // 3、从t_weibo表中获取被关注用户最近的10条微博
            table_weibo = (HTable) conn.getTable(TableName.valueOf(WeiboConstants.NS,WeiboConstants.TABLE_WEIBO));
            PrefixFilter filter = new PrefixFilter(Bytes.toBytes(Math.abs(focusUserId.hashCode() % 9) + "_" + focusUserId));
            Scan scan = new Scan();
            //后续只需要发布微博的rowkey和timestap，因此加上查找列可以少返回数据，提升性能
            scan.addColumn(WeiboConstants.CF1,WeiboConstants.USERID);
            scan.setFilter(filter);
            ResultScanner resultScanner = WeiboHbaseDaoImpl.getDatas(table_weibo, scan);

            // 4、在t_user_weibo_list表中，给当前用户新增被关注用户的最近的10条微博
            List<Cell> cells = new ArrayList<Cell>();
            int count = 0;
            for (Result result : resultScanner) {
                if (count < 10){
                    Cell cell = result.rawCells()[0];
                    cells.add(cell);
                    count++;
                } else {
                    break;
                }
            }
            if (!cells.isEmpty()) {
                table_user_weibo_list = (HTable) conn.getTable(TableName.valueOf(WeiboConstants.NS,WeiboConstants.TABLE_USER_WEIBO_LIST));
                List<Put> putList = new ArrayList<Put>();
                Put put_user_weibo_list = new Put(createUserRowKey(userid));
                for (Cell cell : cells) {
                    put_user_weibo_list.addColumn(WeiboConstants.CF1,focusUserId.getBytes(),cell.getTimestamp(),CellUtil.cloneRow(cell));
                }
                putList.add(put_user_weibo_list);
                WeiboHbaseDaoImpl.PutDatas(table_user_weibo_list,putList);
                System.out.println("用户" + userid + "关注的用户" + focusUserId + "的最近10条微博已同步成功");
            } else {
                System.out.println("用户" + userid + "关注的用户" + focusUserId + "未发布过微博,不需要同步微博到接收列表");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table_relations != null) {
                try {
                    table_relations.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (table_weibo != null) {
                try {
                    table_weibo.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (table_user_weibo_list != null) {
                try {
                    table_user_weibo_list.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 取消关注
     * 步骤：
     * 1、在t_user_relations表中，删除当前用户的该关注用户
     * 2、在t_user_relations表中，删除该被关注用户的当前粉丝用户
     * 3、在t_user_weibo_list表中，删除当前用户下该被关注用户的所有数据(该表中，以被关注用户userid作为列名，所以相当于删除整个列)
     * @param userid
     * @param focusUserId
     */
    public static void deleteFocus(String userid,String focusUserId){
        HTable table_relations = null;
        HTable table_user_weibo_list = null;
        try {

            table_relations = (HTable) conn.getTable(TableName.valueOf(WeiboConstants.NS,WeiboConstants.TABLE_USER_RELATIONS));
            List<Delete> deletes = new ArrayList<Delete>();

            // 1、在t_user_relations表中，删除当前用户的该关注用户
            Delete delete_focus = new Delete(createUserRowKey(userid));
            delete_focus.addColumn(WeiboConstants.CF1,focusUserId.getBytes());
            deletes.add(delete_focus);

            // 2、在t_user_relations表中，删除该被关注用户的当前粉丝用户
            Delete delete_fans = new Delete(createUserRowKey(focusUserId));
            delete_fans.addColumn(WeiboConstants.CF2,userid.getBytes());
            deletes.add(delete_fans);

            WeiboHbaseDaoImpl.deleteDatas(table_relations,deletes);
            System.out.println("用户" + userid + "的关注用户" +  focusUserId + "已删除");
            System.out.println("用户" + focusUserId + "的粉丝用户" +  userid + "已删除");

            // 3、在t_user_weibo_list表中，删除当前用户下该被关注用户的所有数据(该表中，以被关注用户userid作为列名，所以相当于删除整个列)
            table_user_weibo_list = (HTable) conn.getTable(TableName.valueOf(WeiboConstants.NS,WeiboConstants.TABLE_USER_WEIBO_LIST));
            List<Delete> deleteList = new ArrayList<Delete>();
            Delete delete_user_weibo_list = new Delete(createUserRowKey(userid));
            // addColumns() 因为需要删除整个列所有的单元格，所以需要s
            delete_user_weibo_list.addColumns(WeiboConstants.CF1,focusUserId.getBytes());
            deleteList.add(delete_user_weibo_list);
            WeiboHbaseDaoImpl.deleteDatas(table_user_weibo_list,deleteList);
            System.out.println("用户" + userid + "关注的用户" + focusUserId + "的接收列表中所有微博已删除成功");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table_relations != null) {
                try {
                    table_relations.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (table_user_weibo_list != null) {
                try {
                    table_user_weibo_list.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 查询当前用户的所有关注用户的最新100条微博
     * 1、从表t_user_weibo_list中获取当前用户所关注用户的最新100条微博的rowkey
     * 2、根据查到的rowkey从表t_weibo中查询微博具体内容
     * @param userid
     * @return
     */
    public static List<Weibo> getUserWeiboList(String userid){
        List<Weibo> weibo_list = new ArrayList<Weibo>();
        HTable table_user_weibo_list = null;
        HTable table_weibo = null;
        try {
            // 1、从表t_user_weibo_list中获取当前用户所关注用户的最新100条微博的rowkey
            table_user_weibo_list = (HTable) conn.getTable(TableName.valueOf(WeiboConstants.NS,WeiboConstants.TABLE_USER_WEIBO_LIST));
            Get get = new Get(createUserRowKey(userid));
            get.setMaxVersions(100);
            Result rowData = WeiboHbaseDaoImpl.getOneRowData(table_user_weibo_list, get);
            Cell[] cells = rowData.rawCells();

            if (cells != null && cells.length > 0) {
                Arrays.sort(cells, new Comparator<Cell>() {
                    public int compare(Cell o1, Cell o2) {
                        return -(int) (o1.getTimestamp() - o2.getTimestamp());
                    }
                });
                List<byte[]> rowkey_lst = new ArrayList<byte[]>();
                int count = 0;
                for (Cell cell : cells) {
                    if(count < 100){
                        byte[] value = CellUtil.cloneValue(cell);
                        rowkey_lst.add(value);
                    }
                }
                // 2、根据查到的rowkey从表t_weibo中查询微博具体内容
                if (!rowkey_lst.isEmpty()) {
                    table_weibo = (HTable) conn.getTable(TableName.valueOf(WeiboConstants.NS,WeiboConstants.TABLE_WEIBO));
                    for (byte[] rowkey : rowkey_lst) {
                        Get get_weibo = new Get(rowkey);
                        Result result = WeiboHbaseDaoImpl.getOneRowData(table_weibo, get_weibo);
                        if (!result.isEmpty()) {
                            Weibo weibo = new Weibo();
                            weibo.setTitle(Bytes.toString(result.getValue(WeiboConstants.CF1,WeiboConstants.TITLE)));
                            weibo.setContent(Bytes.toString(result.getValue(WeiboConstants.CF1,WeiboConstants.CONTENT)));
                            weibo.setImage(result.getValue(WeiboConstants.CF1,WeiboConstants.IMAGE));
                            weibo.setTime(result.rawCells()[0].getTimestamp());
                            weibo.setUserid(Bytes.toString(result.getValue(WeiboConstants.CF1,WeiboConstants.USERID)));
                            weibo_list.add(weibo);
                        }
                    }
                }
            } else {
                System.out.println("用户" + userid + "接收列表为空");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table_user_weibo_list != null) {
                try {
                    table_user_weibo_list.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (table_weibo != null) {
                try {
                    table_weibo.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return weibo_list;
    }

    /**
     * 创建t_weibo表的rowkey
     * @param userid
     * @return
     */
    private static byte[] createWeiboRowKey(String userid){
        //Long.MAX_VALUE - System.currentTimeMillis() 是为了将同一用户的数据降序排序，因为hbase默认分区region内部升序排列。在获取用户最新微博时就可以获取最前面的微博了。
        return (Math.abs(userid.hashCode() % 9) + "_" + userid + "_" + (Long.MAX_VALUE - System.currentTimeMillis())).getBytes();
    }

    /**
     * 创建t_user_relations表和t_user_weibo_list表的rowkey
     * @param userid
     * @return
     */
    private static byte[] createUserRowKey(String userid){
        return (Math.abs(userid.hashCode() % 9) + "_" + userid).getBytes();
    }

    /**
     * 根据用户userid、日期、title查询微博数据
     * @param userid
     * @param date
     * @param title
     * @return
     */
    public static List<Weibo> getWeiboByUserAndDate(String userid, String date,String title){
        List<Weibo> weibo_list = new ArrayList<Weibo>();
        HTable table = null;
        try {
            table = (HTable) conn.getTable(TableName.valueOf(WeiboConstants.NS,WeiboConstants.TABLE_WEIBO));
            long dayTime=new SimpleDateFormat("yyyyMMdd").parse(date).getTime()+86400000L;
            String startkey=Math.abs(userid.hashCode() % 9) + "_" + userid + "_" + (Long.MAX_VALUE-dayTime);
            String endkey=Math.abs(userid.hashCode() % 9) + "_" + userid + "_" + (Long.MAX_VALUE-dayTime+86400000L);
            Scan scan = new Scan(startkey.getBytes(),endkey.getBytes());

            SingleColumnValueFilter filter = new SingleColumnValueFilter(WeiboConstants.CF1,WeiboConstants.TITLE,CompareFilter.CompareOp.EQUAL,title.getBytes());
            scan.setFilter(filter);

            ResultScanner resultScanner = WeiboHbaseDaoImpl.getDatas(table, scan);
            for (Result result : resultScanner) {
                if (!result.isEmpty()) {
                    Weibo weibo = new Weibo();
                    weibo.setTitle(Bytes.toString(result.getValue(WeiboConstants.CF1,WeiboConstants.TITLE)));
                    weibo.setContent(Bytes.toString(result.getValue(WeiboConstants.CF1,WeiboConstants.CONTENT)));
                    weibo.setImage(result.getValue(WeiboConstants.CF1,WeiboConstants.IMAGE));
                    weibo.setTime(result.rawCells()[0].getTimestamp());
                    weibo.setUserid(Bytes.toString(result.getValue(WeiboConstants.CF1,WeiboConstants.USERID)));
                    weibo_list.add(weibo);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return weibo_list;
    }
}
