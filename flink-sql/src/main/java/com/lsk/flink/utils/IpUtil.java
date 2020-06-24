package com.lsk.flink.utils;

import org.lionsoul.ip2region.DbConfig;
import org.lionsoul.ip2region.DbMakerConfigException;
import org.lionsoul.ip2region.DbSearcher;

import java.io.IOException;


/**
 * IpUtil 工具类
 *
 * @author lsk
 * @class_name IpUtil
 * @date 2020-02-03
 */
public class IpUtil {

    /**
     * 根据 ip 获取 region info
     * <p>
     * 中国|0|上海|上海市|有线通
     *
     * @param ip ip
     * @author red
     */
    public static String getRegionByIp(String ip) {
        DbSearcher searcher = null;
        String rs = null;

        try {
//            String dbPath = Objects.requireNonNull(IpUtil.class.getClassLoader().getResource("ip2region.db")).getPath();
            String dbPath = IpUtil.class.getClass().getResource("/ip2region.db").getPath();
            searcher = new DbSearcher(new DbConfig(), dbPath);
            rs = searcher.binarySearch(ip).getRegion();
        } catch (DbMakerConfigException | IOException | NullPointerException e) {
            e.printStackTrace();
        } finally {
            if (searcher != null) {
                try {
                    searcher.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return rs;
    }

    public static void main(String[] args) {
        System.out.println(getRegionByIp("171.9.174.61"));
    }
}

