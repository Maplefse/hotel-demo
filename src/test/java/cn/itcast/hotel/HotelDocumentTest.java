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
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

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
        request.source(JSON.toJSONString(hotelDoc) , XContentType.JSON);

        // 3. 发送请求
        client.index(request, RequestOptions.DEFAULT);


    }

    /**
     * 批量插入操作
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
                    .source(JSON.toJSONString(hotelDoc) , XContentType.JSON)    //添加数据
            );
            //不仅可以批量插入，也可以批量删除、修改、查询等
        }

        //发送请求
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.hasFailures());

    }

    void search() {

        SearchRequest searchRequest = new SearchRequest();

        
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
