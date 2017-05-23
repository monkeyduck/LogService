package com.xiaole.elasticsearch;

import com.xiaole.mvc.model.NormalLog;
import com.xiaole.mvc.model.checker.simpleChecker.CheckSimpleInterface;
import com.xiaole.mvc.model.checker.simpleChecker.SimpleCheckerFactory;
import net.sf.json.JSONArray;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.joda.time.DateTime;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by llc on 16/12/19.
 */
public class ELServer {
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ELServer.class);
    private static final String serverIp = "10.252.0.171";      // 内网ip
//    private static final String serverIp = "101.201.103.114";
    private static TransportClient client;

    static {
        try{
            Settings settings = Settings.builder()
                    .put("cluster.name", "llc-cluster").build();
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(serverIp), 9300));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void doIndex(){
        String index = "el-2016-12-20";
        String type = "stat";
        IndexResponse response = null;
        try {
            response = client.prepareIndex("el-2016-12-20", "stat")
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("user", "llc")
                            .field("postDate", new DateTime())
                            .field("message", "{\"cmd\":\"\",\"memberId\":\"c6a845ef875144b980cb915c3a0e0a140.8537223\",\"replyModule\":\"combinator\",\"to\":\"user\",\"link\":\"combinator\",\"replyContent\":\"歌谣do re mi\",\"replyType\":\"text\",\"replyConfidence\":1,\"packageId\":1482739135303,\"deviceId\":\"18:97:ff:04:55:c0\"}")
                            .endObject()
                    )
                    .get();
        } catch (Exception e) {
            logger.error(e.getMessage() + "caused by: " + response.toString());
        }
        logger.info("Index a document successfully, index: " + index + ", type: " + type);
    }

    public void doGet() {
        GetResponse response = client.prepareGet("twitter", "tweet", "2").get();
//        System.out.println(response.getSourceAsString());
    }

    public void doSearch() {
        SearchResponse response = client.prepareSearch("twitter")
                .setTypes("tweet")
                .setQuery(QueryBuilders.termQuery("user", ""))   // Query
                .setQuery(QueryBuilders.termQuery("postDate", "2016-12-21T05:49:58.774Z"))
                .setQuery(QueryBuilders.termQuery("message", "trying out Elasticsearch"))
//                .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
                .get();
        logger.info("Response: " + response.toString());
    }

    public List<String> getComplexLogByDate(String memberId, String date, String module, String level, String env,
                                            String src) {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        Map<String, String> fields = new HashMap<>();
        if (!memberId.equals("all")) {
            fields.put("memberId", memberId);
        }
        if (!module.equals("all") && src.equals("xiaole")) {
            fields.put("module", module);
        }
        if (!level.equals("all")) {
            fields.put("level", level);
        }
        if (!env.equals("all")) {
            fields.put("environment", env);
        }
        if (src.equals("beiwa")) {
            fields.put("module", "cockroach");
        }
        for (Map.Entry<String, String> entry : fields.entrySet()){
            boolQuery.must(QueryBuilders.matchQuery(entry.getKey(), entry.getValue()));
        }
        List<String> logList = new ArrayList<>();
        try {
            SearchResponse scrollResp = client.prepareSearch("el-" + date)
                    .setTypes("stat")
                    .setQuery(boolQuery)
                    .setScroll(new TimeValue(60000))
                    .setQuery(boolQuery)
                    .setSize(1000)
                    .get();

            //Scroll until no hits are returned
            do {
                String sourceAsString = "";
                for (SearchHit hit : scrollResp.getHits().getHits()) {
                    //Handle the hit...
                    sourceAsString = hit.getSourceAsString();
                    if (sourceAsString != null) {
                        logList.add(sourceAsString);
                    }
                }
                scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            } while(scrollResp.getHits().getHits().length != 0);

        } catch (Exception e) {
            logger.error(e.getMessage() + "index: " + "el-" + date);
        }
        return logList;
    }

    public String getLogByDateFromELSearch(String date, String memberId, String module, String level, String env) {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        Map<String, String> fields = new HashMap<>();
        if (!memberId.equals("all")) {
            fields.put("memberId", memberId);
        }
        if (!module.equals("all")) {
            fields.put("module", module);
        }
        if (!level.equals("all")) {
            fields.put("level", level);
        }
        if (!env.equals("all")) {
            fields.put("environment", env);
        }
        for (Map.Entry<String, String> entry : fields.entrySet()){
            boolQuery.must(QueryBuilders.matchQuery(entry.getKey(), entry.getValue()));
        }
        try {
            SearchResponse response = client.prepareSearch("el-" + date)
                    .setTypes("stat")
                    .setQuery(boolQuery)
                    .get();
            return response.toString();
        } catch (Exception e) {
            logger.error(e.getMessage() + "index: " + "el-" + date);
            return "Index error: No such index: el-" + date;
        }

    }

    public String getLogBetweenDate(String date, String edate, int range, String memberId, String module, String level,
                                    String env) {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        Map<String, String> fields = new HashMap<>();
        if (!memberId.equals("all")) {
            fields.put("memberId", memberId);
        }
        if (!module.equals("all")) {
            fields.put("module", module);
        }
        if (!level.equals("all")) {
            fields.put("level", level);
        }
        if (!env.equals("all")) {
            fields.put("environment", env);
        }
        for (Map.Entry<String, String> entry : fields.entrySet()){
            boolQuery.must(QueryBuilders.matchQuery(entry.getKey(), entry.getValue()));
        }
        String[] indices = new String[range+1];
        indices[0] = "<el-{" + date + "{yyyy-MM-dd}}>";
        for (int i = 1; i <= range; ++i) {
            indices[i] = "<el-{" + date + "||+" + i + "d{yyyy-MM-dd}}>";
        }
        try {
            SearchResponse response = client.prepareSearch(indices)
                    .setTypes("stat")
                    .setQuery(boolQuery)
                    .get();
            return response.toString();
        } catch (Exception e) {
            logger.error(e.getMessage() + "indices: " + indices[0] + "...");
            return "Index error: No index from " + date + " to " + edate;
        }
    }

    public String getLogBetweenTimeStamp(long sts, long ets, String memberId, String module, String level, String env) {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        Map<String, String> fields = new HashMap<>();
        if (!memberId.equals("all")) {
            fields.put("memberId", memberId);
        }
        if (!module.equals("all")) {
            fields.put("module", module);
        }
        if (!level.equals("all")) {
            fields.put("level", level);
        }
        if (!env.equals("all")) {
            fields.put("environment", env);
        }
        for (Map.Entry<String, String> entry : fields.entrySet()){
            boolQuery.must(QueryBuilders.matchQuery(entry.getKey(), entry.getValue()));
        }
        DateTime sdt = new DateTime(sts);
        DateTime edt = new DateTime(ets);
        String sdate = sdt.toString("yyyy-MM-dd");
        String edate = edt.toString("yyyy-MM-dd");
        if (sdate.equals(edate)) {
            String index = "el-" + sdate;
            try {
                SearchResponse response = client.prepareSearch(index)
                        .setTypes("stat")
                        .setQuery(boolQuery)
                        .setPostFilter(QueryBuilders.rangeQuery("timeStamp").from(sts).to(ets))
                        .get();
                return response.toString();
            } catch (Exception e) {
                logger.error(e.getMessage() + "index: " + index + "...");
                return "Index error: No such index: el-" + sdate;
            }
        } else {    // 时间戳落在两天
            String[] indices = new String[2];
            indices[0] = "<el-{" + sdate + "{yyyy-MM-dd}}>";
            indices[1] = "<el-{" + sdate + "||+" + 1 + "d{yyyy-MM-dd}}>";
            try {
                SearchResponse response = client.prepareSearch(indices)
                        .setTypes("stat")
                        .setQuery(boolQuery)
                        .setPostFilter(QueryBuilders.rangeQuery("timeStamp").from(sts).to(ets))
                        .get();
                return response.toString();
            } catch (Exception e) {
                logger.error(e.getMessage() + "indices: " + indices[0] + "...");
                return "Index error: No index exists from el-" + sdate + " to el-" + edate;
            }
        }
    }

    public JSONArray getChatLogByNumber(int count, String date, String memberId, String level, String env) {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        Map<String, String> fields = new HashMap<>();
        BoolQueryBuilder clauseQuery = new BoolQueryBuilder();
        clauseQuery.should(QueryBuilders.matchQuery("from", "frontend"));
        clauseQuery.should(QueryBuilders.matchQuery("to", "frontend"));
        boolQuery.must(clauseQuery);
        fields.put("module", "cockroach");
        if (!memberId.equals("all")) {
            fields.put("memberId", memberId);
        }
        if (!level.equals("all")) {
            fields.put("level", level);
        }
        if (!env.equals("all")) {
            fields.put("environment", env);
        }
        for (Map.Entry<String, String> entry : fields.entrySet()){
            boolQuery.must(QueryBuilders.matchQuery(entry.getKey(), entry.getValue()));
        }
        try {
            FieldSortBuilder sortBuilder = SortBuilders.fieldSort("timeStamp")
                    .order(SortOrder.DESC);

            SearchResponse response = client.prepareSearch("el-" + date)
                    .setTypes("stat")
                    .setQuery(boolQuery)
                    .addSort(sortBuilder)
                    .setSize(count)
                    .get();
            return convert2JsonArray(response);
        } catch (Exception e) {
            logger.error(e.getMessage() + "index: " + "el-" + date);
            return null;
        }
    }

    public JSONArray getChatLogByTimeStamp(long startTimeStamp, long endTimeStamp, String memberId, String level,
                                           String env) {
        DateTime sdt = new DateTime(startTimeStamp);
        DateTime edt = new DateTime(endTimeStamp);
        String date = sdt.toString("yyyy-MM-dd");
        if (!edt.toString("yyyy-MM-dd").equals(date)) {
            logger.error("时间戳不在同一天");
            List<String> errMsg = new ArrayList<>();
            errMsg.add("时间戳不在同一天");
            return JSONArray.fromObject(errMsg);
        } else {
            BoolQueryBuilder boolQuery = new BoolQueryBuilder();
            Map<String, String> fields = new HashMap<>();
            BoolQueryBuilder clauseQuery = new BoolQueryBuilder();
            clauseQuery.should(QueryBuilders.matchQuery("from", "frontend"));
            clauseQuery.should(QueryBuilders.matchQuery("to", "frontend"));
            boolQuery.must(clauseQuery);
            fields.put("module", "cockroach");
            if (!memberId.equals("all")) {
                fields.put("memberId", memberId);
            }
            if (!level.equals("all")) {
                fields.put("level", level);
            }
            if (!env.equals("all")) {
                fields.put("environment", env);
            }
            for (Map.Entry<String, String> entry : fields.entrySet()){
                boolQuery.must(QueryBuilders.matchQuery(entry.getKey(), entry.getValue()));
            }
            try {
                SearchResponse response = client.prepareSearch("el-" + date)
                        .addSort("timeStamp", SortOrder.ASC)
                        .setTypes("stat")
                        .setQuery(boolQuery)
                        .setPostFilter(QueryBuilders.rangeQuery("timeStamp").from(startTimeStamp).to(endTimeStamp))
                        .get();
                return convert2JsonArray(response);
            } catch (Exception e) {
                logger.error(e.getMessage() + "index: " + "el-" + date);
                return null;
            }
        }
    }

//    public JSONArray getRealTimeLogByDateFromELSearch(String date, String memberId, String module, String level,
//                                                      String env, String src) {
//        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
//        Map<String, String> fields = new HashMap<>();
//        fields.put("memberId", memberId);
////        BoolQueryBuilder clauseQuery = new BoolQueryBuilder();
////        clauseQuery.should(QueryBuilders.matchQuery("from", "frontend"));
////        clauseQuery.should(QueryBuilders.matchQuery("to", "frontend"));
////        boolQuery.must(clauseQuery);
////        fields.put("module", "cockroach");
//        if (!level.equals("all")) {
//            fields.put("level", level);
//        }
//        if (!env.equals("all")) {
//            fields.put("environment", env);
//        }
//        if (!module.equals("all")) {
//            fields.put("module", module);
//        }
//        for (Map.Entry<String, String> entry : fields.entrySet()){
//            boolQuery.must(QueryBuilders.matchQuery(entry.getKey(), entry.getValue()));
//        }
//        FieldSortBuilder sortBuilder = SortBuilders.fieldSort("timeStamp")
//                .order(SortOrder.ASC);
//        List<String> ret = new ArrayList<>();
//        NormalLog log;
//        // 根据日志源是小乐还是贝瓦生成相应的简单日志检查器
//        CheckSimpleInterface simpleChecker = SimpleCheckerFactory.genSimpleChecker(src);
//        try {
//            SearchResponse scrollResp = client.prepareSearch("el-" + date)
//                    .setTypes("stat")
//                    .addSort(sortBuilder)
//                    .setScroll(new TimeValue(60000))
//                    .setQuery(boolQuery)
//                    .setSize(1000)
//                    .get();
//
//            //Scroll until no hits are returned
//            do {
//                String sourceAsString = "";
//                for (SearchHit hit : scrollResp.getHits().getHits()) {
//                    //Handle the hit...
//                    sourceAsString = hit.getSourceAsString();
//                    if (sourceAsString != null) {
//                        if (simpleChecker.checkSimple(sourceAsString)) {
//                            try{
//                                log = new NormalLog(sourceAsString);
//                            }catch (Exception e) {
//                                logger.error("Parse log error when handling hits: " + e.getMessage() + "log: " + sourceAsString);
//                                continue;
//                            }
//                            ret.add(log.toNewSimpleFormat());
//                        }
//
//                    }
//                }
//                scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
//            } while(scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.
//        } catch (Exception e) {
//            logger.error(e.getMessage() + ". No index exists: " + "el-" + date);
//            List<String> list = new ArrayList<>();
//            list.add("查询数据不存在");
//            return JSONArray.fromObject(list);
//        }
//        return JSONArray.fromObject(ret);
//    }

    public List<String> getSimpleLogByDate(String date, String memberId, String env, String level, String src){
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        Map<String, String> fields = new HashMap<>();
        if (src.equals("beiwa")) {
            BoolQueryBuilder clauseQuery = new BoolQueryBuilder();
            clauseQuery.should(QueryBuilders.matchQuery("from", "frontend"));
            clauseQuery.should(QueryBuilders.matchQuery("to", "frontend"));
            boolQuery.must(clauseQuery);
            fields.put("module", "cockroach");
        }

        fields.put("memberId", memberId);
        if (!level.equals("all")) {
            fields.put("level", level);
        }
        if (!env.equals("all")) {
            fields.put("environment", env);
        }
        for (Map.Entry<String, String> entry : fields.entrySet()){
            boolQuery.must(QueryBuilders.matchQuery(entry.getKey(), entry.getValue()));
        }
        FieldSortBuilder sortBuilder = SortBuilders.fieldSort("timeStamp")
                .order(SortOrder.ASC);
        NormalLog log;
        List<String> resultList = new ArrayList<>();
        // 根据日志源是小乐还是贝瓦生成相应的简单日志检查器
        CheckSimpleInterface simpleChecker = SimpleCheckerFactory.genSimpleChecker(src);
        try {
            SearchResponse scrollResp = client.prepareSearch("el-" + date)
                    .setTypes("stat")
                    .addSort(sortBuilder)
                    .setScroll(new TimeValue(60000))
                    .setQuery(boolQuery)
                    .setSize(1000)
                    .get();

            //Scroll until no hits are returned
            do {
                String sourceAsString = "";
                for (SearchHit hit : scrollResp.getHits().getHits()) {
                    //Handle the hit...
                    sourceAsString = hit.getSourceAsString();
                    if (sourceAsString != null) {
                        if (simpleChecker.checkSimple(sourceAsString)) {
                            try{
                                log = new NormalLog(sourceAsString);
                            }catch (Exception e) {
                                logger.error("Parse log error when handling hits: " + e.getMessage() + "log: " + sourceAsString);
                                continue;
                            }
                            resultList.addAll(log.toNewSimpleFormat());
                        }
                    }
                }
                scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            } while(scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.
        } catch (Exception e) {
            logger.error(e.getMessage() + "index: " + "el-" + date);
            return null;
        }
        return resultList;
    }

    public Map<String, List<String>> getAllUserSimpleLog(String date, String env, String level, String src) {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        Map<String, String> fields = new HashMap<>();
        if (src.equals("beiwa")) {
            BoolQueryBuilder clauseQuery = new BoolQueryBuilder();
            clauseQuery.should(QueryBuilders.matchQuery("from", "frontend"));
            clauseQuery.should(QueryBuilders.matchQuery("to", "frontend"));
            boolQuery.must(clauseQuery);
            fields.put("module", "cockroach");
        }
        if (!level.equals("all")) {
            fields.put("level", level);
        }
        if (!env.equals("all")) {
            fields.put("environment", env);
        }
        for (Map.Entry<String, String> entry : fields.entrySet()){
            boolQuery.must(QueryBuilders.matchQuery(entry.getKey(), entry.getValue()));
        }
        FieldSortBuilder sortBuilder = SortBuilders.fieldSort("timeStamp")
                .order(SortOrder.ASC);
        NormalLog log;
        Map<String, List<String>> resultMap = new HashMap<>();
        // 根据日志源是小乐还是贝瓦生成相应的简单日志检查器
        CheckSimpleInterface simpleChecker = SimpleCheckerFactory.genSimpleChecker(src);
        try {
            SearchResponse scrollResp = client.prepareSearch("el-" + date)
                    .setTypes("stat")
                    .addSort(sortBuilder)
                    .setScroll(new TimeValue(60000))
                    .setQuery(boolQuery)
                    .setSize(1000)
                    .get();

            //Scroll until no hits are returned
            do {
                String sourceAsString = "";
                for (SearchHit hit : scrollResp.getHits().getHits()) {
                    //Handle the hit...
                    sourceAsString = hit.getSourceAsString();
                    if (sourceAsString != null) {
                        if (simpleChecker.checkSimple(sourceAsString)) {
                            try{
                                log = new NormalLog(sourceAsString);
                            }catch (Exception e) {
                                logger.error("Parse log error when handling hits: " + e.getMessage() + "log: " + sourceAsString);
                                continue;
                            }
                            if (!resultMap.containsKey(log.getMember_id())) {
                                resultMap.put(log.getMember_id(), new ArrayList<>());
                            }
                            resultMap.get(log.getMember_id()).addAll(log.toNewSimpleFormat());
                        }
                    }
                }

                scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            } while(scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.
        } catch (Exception e) {
            logger.error(e.getMessage() + "index: " + "el-" + date);
            return null;
        }
        return resultMap;
    }

    public Map<String, List<String>> getAllUserInstantLog(String date, String env, String level, String src) {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        Map<String, String> fields = new HashMap<>();

        BoolQueryBuilder clauseQuery = new BoolQueryBuilder();
        clauseQuery.should(QueryBuilders.matchQuery("from", "instant_chat"));
        clauseQuery.should(QueryBuilders.matchQuery("to", "instant_chat"));
        boolQuery.must(clauseQuery);
        fields.put("module", "cockroach");

        if (!level.equals("all")) {
            fields.put("level", level);
        }
        if (!env.equals("all")) {
            fields.put("environment", env);
        }
        for (Map.Entry<String, String> entry : fields.entrySet()){
            boolQuery.must(QueryBuilders.matchQuery(entry.getKey(), entry.getValue()));
        }
        FieldSortBuilder sortBuilder = SortBuilders.fieldSort("timeStamp")
                .order(SortOrder.ASC);
        NormalLog log;
        Map<String, List<String>> resultMap = new HashMap<>();
        try {
            SearchResponse scrollResp = client.prepareSearch("el-" + date)
                    .setTypes("stat")
                    .addSort(sortBuilder)
                    .setScroll(new TimeValue(60000))
                    .setQuery(boolQuery)
                    .setSize(1000)
                    .get();

            //Scroll until no hits are returned
            do {
                String sourceAsString = "";
                for (SearchHit hit : scrollResp.getHits().getHits()) {
                    //Handle the hit...
                    sourceAsString = hit.getSourceAsString();
                    if (sourceAsString != null) {
                        try{
                            log = new NormalLog(sourceAsString);
                        }catch (Exception e) {
                            logger.error("Parse log error when handling hits: " + e.getMessage() + "log: " + sourceAsString);
                            continue;
                        }
                        if (!resultMap.containsKey(log.getMember_id())) {
                            resultMap.put(log.getMember_id(), new ArrayList<>());
                        }
                        resultMap.get(log.getMember_id()).add(sourceAsString);
                    }
                }
                scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            } while(scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.
        } catch (Exception e) {
            logger.error(e.getMessage() + "index: " + "el-" + date);
            return null;
        }
        return resultMap;
    }


    public Map<String, List<String>> getAllUserInstantSimpleLog(String date, String env, String level, String src) {
        Map<String, List<String>> complexLogMap = getAllUserInstantLog(date, env, level, src);
        Map<String, List<String>> simpleLogMap = new HashMap<>();
        CheckSimpleInterface simpleChecker = SimpleCheckerFactory.genSimpleChecker(src);
        NormalLog log;
        for (String member_id: complexLogMap.keySet()) {
            List<String> list = complexLogMap.get(member_id);
            for (String comLog: list) {
                if (simpleChecker.checkSimple(comLog)) {
                    try{
                        log = new NormalLog(comLog);
                    }catch (Exception e) {
                        logger.error("Parse log error when downloading instant chat log: " + e.getMessage() + ", log: " + comLog);
                        continue;
                    }
                    if (!simpleLogMap.containsKey(log.getMember_id())) {
                        simpleLogMap.put(log.getMember_id(), new ArrayList<>());
                    }
                    simpleLogMap.get(log.getMember_id()).addAll(log.toNewSimpleFormat());
                }
            }
        }
        return simpleLogMap;
    }

    public List<String> getInstantLogByDate(String date, String memberId, String env, String level, String src) {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        Map<String, String> fields = new HashMap<>();

        BoolQueryBuilder clauseQuery = new BoolQueryBuilder();
        clauseQuery.should(QueryBuilders.matchQuery("from", "instant_chat"));
        clauseQuery.should(QueryBuilders.matchQuery("to", "instant_chat"));
        boolQuery.must(clauseQuery);
        fields.put("module", "cockroach");

        fields.put("memberId", memberId);
        if (!level.equals("all")) {
            fields.put("level", level);
        }
        if (!env.equals("all")) {
            fields.put("environment", env);
        }

        for (Map.Entry<String, String> entry : fields.entrySet()){
            boolQuery.must(QueryBuilders.matchQuery(entry.getKey(), entry.getValue()));
        }
        List<String> logList = new ArrayList<>();
        try {
            SearchResponse scrollResp = client.prepareSearch("el-" + date)
                    .setTypes("stat")
                    .setQuery(boolQuery)
                    .setScroll(new TimeValue(60000))
                    .setQuery(boolQuery)
                    .setSize(1000)
                    .get();

            //Scroll until no hits are returned
            do {
                String sourceAsString = "";
                for (SearchHit hit : scrollResp.getHits().getHits()) {
                    //Handle the hit...
                    sourceAsString = hit.getSourceAsString();
                    if (sourceAsString != null) {
                        logList.add(sourceAsString);
                    }
                }
                scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            } while(scrollResp.getHits().getHits().length != 0);

        } catch (Exception e) {
            logger.error(e.getMessage() + "index: " + "el-" + date);
        }
        return logList;
    }

    public List<String> getSimpleInstantLogByDate(String date, String memberId, String env, String level, String src) {
        List<String> complexLogList = getInstantLogByDate(date, memberId, env, level, src);
        List<String> simpleLogList = new ArrayList<>();
        CheckSimpleInterface simpleChecker = SimpleCheckerFactory.genSimpleChecker(src);
        NormalLog log;
        for (String comLog: complexLogList) {
            if (simpleChecker.checkSimple(comLog)) {
                try{
                    log = new NormalLog(comLog);
                }catch (Exception e) {
                    logger.error("Parse log error when downloading simple instant chat log: " + e.getMessage() + ", log: " + comLog);
                    continue;
                }
                simpleLogList.addAll(log.toNewSimpleFormat());
            }
        }
        return simpleLogList;
    }

    private JSONArray convert2JsonArray(SearchResponse response) {
        logger.info("Start to convert response to json ...");
        if (response == null) logger.info("response is null.");
        List<String> list = new ArrayList<>();
        try {
            SearchHit[] results = response.getHits().getHits();
            String sourceAsString = "";
            for (SearchHit hit : results) {
                sourceAsString = hit.getSourceAsString();
                if (sourceAsString != null) {
                    list.add(sourceAsString);
                }
            }
        } catch (Exception e) {
            logger.error("Convert to JsonArray error: " + e.getMessage());
        }
        JSONArray jsonArray = JSONArray.fromObject(list);
        return jsonArray;
    }

    public static void main(String[] args) {
        ELServer server = new ELServer();
        server.doIndex();
    }

}
