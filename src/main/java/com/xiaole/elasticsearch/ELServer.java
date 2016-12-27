package com.xiaole.elasticsearch;

import net.sf.json.JSONArray;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(ELServer.class);
    private static final String serverIp = "10.252.0.171";
//    private static final String serverIp = "101.201.103.114";
    private static final String serverPort = "9300";
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

    public String getLogByDateFromELSearch(String date, String memberId, String module, String level) {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        Map<String, String> fields = new HashMap<>();
        if (!memberId.equals("all")) {
            fields.put("memberId", memberId);
        }
        if (!module.equals("all")) {
            fields.put("module", module);
        }
        if (!level.equals("all")) {
            fields.put("level", level.toUpperCase());
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

    public String getLogBetweenDate(String date, String edate, int range, String memberId, String module, String level) {
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

    public String getLogBetweenTimeStamp(long sts, long ets, String memberId, String module, String level) {
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

    public String getLogByDateFromELSearchSimple(String date) {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        Map<String, String> fields = new HashMap<>();
        for (Map.Entry<String, String> entry : fields.entrySet()){
            boolQuery.must(QueryBuilders.matchQuery(entry.getKey(), entry.getValue()));
        }
        SearchResponse response = client.prepareSearch("el-" + date)
                .setTypes("stat")
                .setQuery(boolQuery)
                .get();
        return response.toString();
    }


    private JSONArray convert2JsonArray(SearchResponse response) {
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
