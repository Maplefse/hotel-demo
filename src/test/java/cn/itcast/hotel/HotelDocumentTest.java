package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@SpringBootTest
class HotelDocumentTest {

    private RestHighLevelClient client;

    @Autowired
    private IHotelService hotelService;

    @Test
    void testAddDocument() throws IOException {

        Hotel hotel = hotelService.getById(61083L);
        HotelDoc hotelDoc = new HotelDoc(hotel);

        // 1. 准备Request对象
        IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());    //链式编程,设置文档的id

        // 2. 准备Json文档
        request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);

        // 3. 发送请求
        client.index(request, RequestOptions.DEFAULT);


    }

    /**
     * 批量插入操作
     *
     * @throws IOException
     */
    @Test
    void testBulkRequest() throws IOException {

        List<Hotel> list = hotelService.list();

        //创建Request
        BulkRequest bulkRequest = new BulkRequest();

        //准备参数
        for (Hotel hotel : list) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            bulkRequest.add(new IndexRequest("hotel")
                    .id(hotelDoc.getId().toString())        //设置id
                    .source(JSON.toJSONString(hotelDoc), XContentType.JSON)    //添加数据
            );
            //不仅可以批量插入，也可以批量删除、修改、查询等
        }

        //发送请求
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.hasFailures());

    }

    /**
     * match_all 查询全部
     */
    @Test
    void search() throws IOException {

        // 还是老三样

        // 1.准备Request
        SearchRequest searchRequest = new SearchRequest("hotel");

        // 2.准备DSL
        SearchSourceBuilder query = searchRequest.source().query(QueryBuilders.matchAllQuery());

        // 3.发送请求
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);


        extracted(search);


    }

    /**
     * 解析查询结果
     * @param search    请求响应结果
     */
    private void extracted(SearchResponse search) {
        //开始解析结果
        //1.获得响应的hits内数据
        SearchHits searchhits = search.getHits();

        //2.得到查询出的总条数
        long value = searchhits.getTotalHits().value;
        System.out.println("查询结果数为:"+value);

        //3.获取文档数组
        SearchHit[] hits = searchhits.getHits();

        //4.遍历
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();

            //反序列化JSON
            HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);

            //获取高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            //获取高亮结果
            HighlightField name = highlightFields.get("name");
            if ( !StringUtils.isEmpty(name)) {  //验证获取到的高亮参数不为空
                //获取高亮值
                String string = name.getFragments()[0].string();    //name.getFragments()为Text类型的数组，获取数组第一个值
                hotelDoc.setName(string);
            }

            System.out.println(hotelDoc);

        }
    }

    /**
     * match查询
     */
    @Test
    void match() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");

        SearchSourceBuilder name = searchRequest.source().query(QueryBuilders.matchQuery(
                "name", "7天"
        ));

        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);

        extracted(search);

    }


    /**
     * 复合bool查询
     */
    @Test
    void testBool() throws IOException {

        // 1.准备Request
        SearchRequest searchRequest = new SearchRequest("hotel");

        // 2.准备DSL

        // 2.1 准备Boolean Query

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        //添加查询条件
        boolQueryBuilder.must(QueryBuilders.termQuery("city", "深圳"));   //城市必须在深圳
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").lte(250));     //价格小于250

        SearchSourceBuilder query = searchRequest.source().query(boolQueryBuilder);

        // 3.发送请求
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);

        extracted(search);

    }

    /**
     * 分页与排序
     */
    @Test
    void pageAndSort() throws IOException {

        // 1.准备Request
        SearchRequest searchRequest = new SearchRequest("hotel");

        // 2.准备DSL
        searchRequest.source().query(QueryBuilders.matchAllQuery());
        searchRequest.source().from(0).size(20).sort("price", SortOrder.ASC);       //分页0-20,并对price升序排序


        // 3.发送请求
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);


        extracted(search);

    }

    /**
     * 高亮
     */
    @Test
    void highLighter() throws IOException {
        // 1.准备Request
        SearchRequest searchRequest = new SearchRequest("hotel");

        // 2.准备DSL
        searchRequest.source().query(QueryBuilders.matchQuery("all", "7天"));
        searchRequest.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));
        // 设置name字段高亮且因为与查询字段不同,将FieldMatch设置为false

        // 3.发送请求
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);


        extracted(search);
    }


    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.1.107:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }


}
