package cn.itcast.hotel;

import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: fxiao
 * @Version: 2022/05/29/18:10
 */
@SpringBootTest
public class HotelSearchTest {

    private RestHighLevelClient client;


    @Autowired
    private IHotelService iHotelService;

    @Test
    void testAggregation() throws IOException {
        // 1. 准备request
        SearchRequest searchRequest = new SearchRequest("hotel");

        // 2. 准备dsl
        searchRequest.source().size(0);     //查询文档数量
        searchRequest.source().aggregation(
                AggregationBuilders.terms("brand")  //聚合名
                        .field("brand")     //聚合字段
                        .size(10)           //聚合结果数量
        );

        // 3. 发送请求
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);

        // 4. 解析结果
        Aggregations aggregations = search.getAggregations();
        // 4.1 根据自定义的聚合名称获取结果
        Terms terms = aggregations.get("brand");
        // 4.2 获取buckets
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            // 4.4 获取key
            String key = bucket.getKeyAsString();
            System.out.println("聚合结果名称:" + key);
            Number keyAsNumber = bucket.getDocCount();
            System.out.println("聚合结果数量:" + keyAsNumber);
        }

    }

    @Test
    void contextLoad(){
        //iHotelService.filters(RequestParams params);
    }


    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.1.104:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }


}
