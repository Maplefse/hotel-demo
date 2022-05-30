package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private RestHighLevelClient client;


    @Override
    public PageResult search(RequestParams params) {
        // 1.准备Request
        SearchRequest searchRequest = new SearchRequest("hotel");

        // 2.准备DSL
        // 2.1 query
        buildBasicQuery(params, searchRequest);



        // 2.2 分页
        searchRequest.source().from((params.getPage() - 1) * params.getSize()).size(params.getSize());

        // 2.3 排序
        String location = params.getLocation();
        if (StringUtils.isEmpty(location)){
            //为空,给个默认经纬度
            location = "31.03463,121.61245";
            searchRequest.source().sort(
                    SortBuilders.geoDistanceSort("location", new GeoPoint(location))
                            .order(SortOrder.ASC)       // 排序方式
                            .unit(DistanceUnit.KILOMETERS)      // 排序单位 KM
            );
        }else{
            searchRequest.source().sort(
                    SortBuilders.geoDistanceSort("location", new GeoPoint(location))    //进行排序的字段与要搜索的经纬度
                            .order(SortOrder.ASC)       // 排序方式
                            .unit(DistanceUnit.KILOMETERS)      // 排序单位 KM
            );
        }

        // 3.发送请求
        SearchResponse search = null;

        try {
            search = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }


        assert search != null;

        return extracted(search);
    }

    @Override
    public Map<String, List<String>> filters(RequestParams params) {
        // 1. 准备request
        SearchRequest searchRequest = new SearchRequest("hotel");

        // 2. 准备dsl
        buildBasicQuery(params, searchRequest);
        searchRequest.source().size(0);     //查询文档数量
        // 2.1 聚合
        bulidAggregation(searchRequest);

        // 3. 发送请求
        SearchResponse search = null;
        try {
            search = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 4. 解析结果
        Aggregations aggregations = search.getAggregations();

        Map<String, List<String>> result = new HashMap<>();

        List<String> brandAgg = getAggByNames("brandAgg", aggregations);

        List<String> cityAgg = getAggByNames("cityAgg", aggregations);

        List<String> starAgg = getAggByNames("starAgg", aggregations);

        result.put("品牌", brandAgg);
        result.put("城市", cityAgg);
        result.put("星级", starAgg);

        return result;
    }

    private List<String> getAggByNames(String aggName, Aggregations aggregations) {
        // 4.1 根据自定义的聚合名称获取结果
        Terms terms = aggregations.get(aggName);

        // 4.2 获取buckets
        List<? extends Terms.Bucket> buckets = terms.getBuckets();

        List<String> aggList = new ArrayList<>();
        for (Terms.Bucket bucket : buckets) {
            // 4.4 获取key
            String key = bucket.getKeyAsString();
            aggList.add(key);
            System.out.println("聚合结果名称:" + key);
            Number keyAsNumber = bucket.getDocCount();
            System.out.println("聚合结果数量:" + keyAsNumber);
        }

        return aggList;
    }

    private void bulidAggregation(SearchRequest searchRequest) {
        searchRequest.source().aggregation(
                AggregationBuilders.terms("brandAgg")  //聚合名
                        .field("brand")     //聚合字段
                        .size(100)           //聚合结果数量
        );

        searchRequest.source().aggregation(
                AggregationBuilders.terms("cityAgg")  //聚合名
                        .field("city")     //聚合字段
                        .size(100)           //聚合结果数量
        );

        searchRequest.source().aggregation(
                AggregationBuilders.terms("starAgg")  //聚合名
                        .field("starName")     //聚合字段
                        .size(100)           //聚合结果数量
        );
    }

    private void buildBasicQuery(RequestParams params, SearchRequest searchRequest) {
        // 构建BooleanQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        // 关键字搜索
        String key = params.getKey();
        if (StringUtils.isEmpty(key)) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("all", key));
        }

        //条件过滤
        // 城市条件
        if (!StringUtils.isEmpty(params.getCity())) {
            boolQuery.filter(QueryBuilders.termQuery("city", params.getCity()));
        }

        // 品牌条件
        if (!StringUtils.isEmpty(params.getBrand())) {
            boolQuery.filter(QueryBuilders.termQuery("brand", params.getBrand()));
        }

        // 星级条件
        if (!StringUtils.isEmpty(params.getStarName())) {
            boolQuery.filter(QueryBuilders.termQuery("starName", params.getStarName()));
        }

        // 价格条件
        if (!StringUtils.isEmpty(params.getMaxPrice()) && !StringUtils.isEmpty(params.getMinPrice())) {
            boolQuery.filter(QueryBuilders.rangeQuery("price")
                    .gte(params.getMinPrice())
                    .lte(params.getMaxPrice()));
        }

        // 2.算分控制
        FunctionScoreQueryBuilder functionScoreQueryBuilder =
                QueryBuilders.functionScoreQuery(   //构建functionQuery
                        boolQuery,  // 原始查询，相关性算分的查询
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{  // function score的数组
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(    // 其中的一个function score 元素
                                        QueryBuilders.termQuery("isAD", true),  // 过滤条件
                                        ScoreFunctionBuilders.weightFactorFunction(100)  // 算分函数
                                )
                        }
        );

        searchRequest.source().query(functionScoreQueryBuilder);

    }

    /**
     * 解析查询结果
     *
     * @param search 请求响应结果
     */
    private PageResult extracted(SearchResponse search) {
        //开始解析结果
        //1.获得响应的hits内数据
        SearchHits searchhits = search.getHits();

        //2.得到查询出的总条数
        long value = searchhits.getTotalHits().value;
        System.out.println("查询结果数为:" + value);

        //3.获取文档数组
        SearchHit[] hits = searchhits.getHits();

        ArrayList<HotelDoc> hotelDocs = new ArrayList<>();

        //4.遍历
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();

            //反序列化JSON
            HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);

            //获取排序值
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length>0){
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }

            //获取高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            //获取高亮结果
            HighlightField name = highlightFields.get("name");
            if (!StringUtils.isEmpty(name)) {  //验证获取到的高亮参数不为空
                //获取高亮值
                String string = name.getFragments()[0].string();    //name.getFragments()为Text类型的数组，获取数组第一个值
                hotelDoc.setName(string);
            }

            hotelDocs.add(hotelDoc);

        }
        return new PageResult(value, hotelDocs);
    }

}
