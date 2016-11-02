package com.birdben.mapreduce.adlog.parser;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * 类描述:
 * 解析广告红包日志
 *
 * @author liuben
 * @version 1.0
 * @since 1.0
 **/
public class AdLogParser {

    private static Log logger = LogFactory.getLog(AdLogParser.class);

    public static final String CLICK_AD = "birdben.ad.click_ad";
    public static final String VIEW_AD = "birdben.ad.view_ad";
    public static final String OPEN_AD = "birdben.ad.open_hb";

    public static final String AD_CLICK_LOG_LIST = "adClickLogList";
    public static final String AD_VIEW_LOG_LIST = "adViewLogList";
    public static final String AD_OPEN_LOG_LIST = "adOpenLogList";

    /**
     * 方法描述
     * 解析广告红包日志,并更新广告红包数据库的打开,展现,点击时间
     *
     * @param
     * @return
     * @throws
     * @author liuben
     * @date 16/10/20 16:33
     **/
    public static List<String> convertLogToAd(String log) throws Exception {
        System.out.println("birdben AdLogParser out start");
        logger.info("birdben AdLogParser logger start");

        List<String> adLogList = new ArrayList<>();
        JSONObject jsonObject = JSON.parseObject(log);
        JSONArray jsonArray = (JSONArray) jsonObject.get("logs");
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject innerJsonObject = (JSONObject) jsonArray.get(i);
            String name = innerJsonObject.get("name").toString();

            logger.info("convertLogToAd name:" + name);

            try {
                if (CLICK_AD.equals(name) || VIEW_AD.equals(name) || OPEN_AD.equals(name)) {
                    String rpid = innerJsonObject.get("rpid").toString();
                    Long inner_timestamp = Long.parseLong(innerJsonObject.get("timestamp").toString());
                    String bid = innerJsonObject.get("bid").toString();
                    String uid = innerJsonObject.get("uid").toString();
                    String did = innerJsonObject.get("did").toString();
                    String duid = innerJsonObject.get("duid").toString();
                    String hb_uid = innerJsonObject.get("hb_uid").toString();
                    String ua = innerJsonObject.get("ua").toString();
                    String device_id = innerJsonObject.get("device_id").toString();
                    String server_timestamp = innerJsonObject.get("server_timestamp").toString();

                    Map<String, Object> map = new HashMap<>();
                    map.put("rpid", rpid);
                    map.put("timestamp", inner_timestamp);
                    map.put("bid", bid);
                    map.put("uid", uid);
                    map.put("did", did);
                    map.put("duid", duid);
                    map.put("hb_uid", hb_uid);
                    map.put("ua", ua);
                    map.put("device_id", device_id);
                    map.put("server_timestamp", server_timestamp);
                    adLogList.add(JSON.toJSONString(map));
                }
            } catch (Exception ex) {
                logger.error("birdben AdLogParser error:" + ex.getMessage());
                ex.printStackTrace();
            }
        }
        return adLogList;
    }
}
