package com.gtl.hbase.weibo;

public class WeiboConstants {
    public static final byte[] NS = "weibo".getBytes();

    public static final byte[] TABLE_WEIBO = "t_weibo".getBytes();
    public static final byte[] TABLE_USER_RELATIONS = "t_user_relations".getBytes();
    public static final byte[] TABLE_USER_WEIBO_LIST = "t_user_weibo_list".getBytes();

    public static final byte[] CF1 = "cf1".getBytes();
    public static final byte[] CF2 = "cf2".getBytes();

    public static final byte[] TITLE = "title".getBytes();
    public static final byte[] CONTENT = "content".getBytes();
    public static final byte[] IMAGE = "image".getBytes();
    public static final byte[] USERID = "userid".getBytes();

    public static final byte[] DEFAULT_VALUE = "default_value".getBytes();
}
