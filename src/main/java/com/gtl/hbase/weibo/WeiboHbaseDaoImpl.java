package com.gtl.hbase.weibo;

import com.sun.org.apache.bcel.internal.generic.ConversionInstruction;
import org.apache.commons.io.filefilter.FalseFileFilter;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class WeiboHbaseDaoImpl {

    /**
     * 判断命名空间是否存在
     * @param ns
     * @param admin
     * @return
     * @throws IOException
     */
    public static boolean namespaceExists(byte[] ns, HBaseAdmin admin){
        // 使用getNamespaceDescriptor()方法获取NamespaceDescriptor时，
        // 如果传入的命名空间不存在，则会抛出NamespaceNotFoundException，如果存在，则返回该命名空间的NamespaceDescriptor。
        // 因此，不能根据返回值是否为null来判断该命名空间是否存在，而是通过捕获NamespaceNotFoundException来判断是否存在

        /* 错误示例
        NamespaceDescriptor namespaceDescriptor = admin.getNamespaceDescriptor(Bytes.toString(ns));
        if (namespaceDescriptor == null) {
            return false;
        }
        return true;
        */

        //正确示例一：使用getNamespaceDescriptor()
        /*
        boolean exists = true;
        try {
            NamespaceDescriptor namespaceDescriptor = admin.getNamespaceDescriptor(Bytes.toString(ns));
        } catch (NamespaceNotFoundException e) {
            exists = false;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return exists;
        */

        //正确示例二：使用
        boolean exists = false;
        try {
            NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
            for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
                if (namespaceDescriptor.getName().equals(Bytes.toString(ns))) {
                    exists = true;
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return exists;
    }

    /**
     * 创建命名空间
     * @param ns
     * @param admin
     * @throws IOException
     */
    public static void createNamespace(byte[] ns,HBaseAdmin admin) throws IOException {
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(Bytes.toString(ns)).build();
        admin.createNamespace(namespaceDescriptor);
    }

    /**
     * 判断表是否存在
     * @param ns
     * @param admin
     * @return
     * @throws IOException
     */
    public static boolean tableExists(byte[] ns,byte[] tn,HBaseAdmin admin) throws IOException {
        boolean exists = admin.tableExists(TableName.valueOf(Bytes.toString(ns) + ":" + Bytes.toString(tn)));
        return exists;
    }

    /**
     * 创建表：预分区
     * @param ns
     * @param tn
     * @param cfs
     * @param versions
     * @param admin
     * @throws IOException
     */
    public static void createTable(byte[] ns, byte[] tn, byte[][] cfs, int[] versions, HBaseAdmin admin) throws IOException {
        //hbase集群有三个节点，最好将分区数设置为节点数量的整倍数，此处设置9个分区
        byte[][] splitKeys = {"0|".getBytes(),"1|".getBytes(),"2|".getBytes(),"3|".getBytes(),"4|".getBytes(),"5|".getBytes(),
                "6|".getBytes(),"7|".getBytes()};

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(Bytes.toString(ns) + ":" + Bytes.toString(tn)));
        for (int i = 0; i < cfs.length; i++) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(cfs[i]);
            columnDescriptor.setMaxVersions(versions[i]);//设置列族的能够保存的最大版本数量
            columnDescriptor.setMinVersions(versions[i]);
            tableDescriptor.addFamily(columnDescriptor);
        }
        admin.createTable(tableDescriptor,splitKeys);
    }

    /**
     * 新增数据
     * @param hTable
     * @param puts
     * @throws IOException
     */
    public static void PutDatas(HTable hTable, List<Put> puts) throws IOException {
        hTable.put(puts);
    }

    /**
     * 删除数据
     * @param hTable
     * @param deletes
     * @throws IOException
     */
    public static void deleteDatas(HTable hTable, List<Delete> deletes) throws IOException {
        hTable.delete(deletes);
    }

    /**
     * 查询一行数据
     * @param hTable
     * @param get
     * @return
     * @throws IOException
     */
    public static Result getOneRowData(HTable hTable, Get get) throws IOException {
        Result result = hTable.get(get);
        return result;
    }

    /**
     * 查询多行数据
     * @param hTable
     * @param scan
     * @return
     * @throws IOException
     */
    public static ResultScanner getDatas(HTable hTable, Scan scan) throws IOException {
        ResultScanner resultScanner = hTable.getScanner(scan);
        return  resultScanner;
    }
}
