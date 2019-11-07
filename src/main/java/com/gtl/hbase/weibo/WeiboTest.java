package com.gtl.hbase.weibo;

import java.util.List;

public class WeiboTest {
    public static void main(String[] args) {
        //测试：创建命名空间
        //WeiboService.createNamespace(WeiboConstants.NS);

        //测试：创建表t_weibo
        //WeiboService.createTable(WeiboConstants.NS, WeiboConstants.TABLE_WEIBO, new byte[][]{WeiboConstants.CF1}, new int[]{1});

        //测试：创建表t_user_relations
        //WeiboService.createTable(WeiboConstants.NS, WeiboConstants.TABLE_USER_RELATIONS, new byte[][]{WeiboConstants.CF1,WeiboConstants.CF2}, new int[]{1,1});

        //测试：创建表t_user_weibo_list
        //WeiboService.createTable(WeiboConstants.NS, WeiboConstants.TABLE_USER_WEIBO_LIST, new byte[][]{WeiboConstants.CF1}, new int[]{100});

        //测试：上传微博
        //WeiboService.uploadWeibo("zhaoliu","吃饭了吗?","zhaoliu_weibo2","");


        //测试：添加关注用户
        //WeiboService.addFocus("zhangsan","zhaoliu");

        //测试：删除关注
        WeiboService.deleteFocus("zhangsan","zhaoliu");

        //测试：微博列表
//        List<Weibo> weiboList =WeiboService.getUserWeiboList("zhangsan");
//        for(Weibo weibo:weiboList){
//            System.out.println("title："+weibo.getTitle()+"" + " time: "+weibo.getTime() + " content : " + weibo.getContent());
//        }

        //测试：filter
//        List<Weibo> weiboList2 =WeiboService.getWeiboByUserAndDate("lisi","20191107","吃饭了吗?");
//        for(Weibo weibo:weiboList2){
//            System.out.println("title："+weibo.getTitle()+"" + " time: "+weibo.getTime());
//        }
    }
}

