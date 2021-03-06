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
        // 1.??????Request
        SearchRequest searchRequest = new SearchRequest("hotel");

        // 2.??????DSL
        // 2.1 query
        buildBasicQuery(params, searchRequest);



        // 2.2 ??????
        searchRequest.source().from((params.getPage() - 1) * params.getSize()).size(params.getSize());

        // 2.3 ??????
        String location = params.getLocation();
        if (StringUtils.isEmpty(location)){
            //??????,?????????????????????
            location = "31.03463,121.61245";
            searchRequest.source().sort(
                    SortBuilders.geoDistanceSort("location", new GeoPoint(location))
                            .order(SortOrder.ASC)       // ????????????
                            .unit(DistanceUnit.KILOMETERS)      // ???????????? KM
            );
        }else{
            searchRequest.source().sort(
                    SortBuilders.geoDistanceSort("location", new GeoPoint(location))    //?????????????????????????????????????????????
                            .order(SortOrder.ASC)       // ????????????
                            .unit(DistanceUnit.KILOMETERS)      // ???????????? KM
            );
        }

        // 3.????????????
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
        // 1. ??????request
        SearchRequest searchRequest = new SearchRequest("hotel");

        // 2. ??????dsl
        buildBasicQuery(params, searchRequest);
        searchRequest.source().size(0);     //??????????????????
        // 2.1 ??????
        bulidAggregation(searchRequest);

        // 3. ????????????
        SearchResponse search = null;
        try {
            search = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 4. ????????????
        Aggregations aggregations = search.getAggregations();

        Map<String, List<String>> result = new HashMap<>();

        List<String> brandAgg = getAggByNames("brandAgg", aggregations);

        List<String> cityAgg = getAggByNames("cityAgg", aggregations);

        List<String> starAgg = getAggByNames("starAgg", aggregations);

        result.put("??????", brandAgg);
        result.put("??????", cityAgg);
        result.put("??????", starAgg);

        return result;
    }

    private List<String> getAggByNames(String aggName, Aggregations aggregations) {
        // 4.1 ??????????????????????????????????????????
        Terms terms = aggregations.get(aggName);

        // 4.2 ??????buckets
        List<? extends Terms.Bucket> buckets = terms.getBuckets();

        List<String> aggList = new ArrayList<>();
        for (Terms.Bucket bucket : buckets) {
            // 4.4 ??????key
            String key = bucket.getKeyAsString();
            aggList.add(key);
            System.out.println("??????????????????:" + key);
            Number keyAsNumber = bucket.getDocCount();
            System.out.println("??????????????????:" + keyAsNumber);
        }

        return aggList;
    }

    private void bulidAggregation(SearchRequest searchRequest) {
        searchRequest.source().aggregation(
                AggregationBuilders.terms("brandAgg")  //?????????
                        .field("brand")     //????????????
                        .size(100)           //??????????????????
        );

        searchRequest.source().aggregation(
                AggregationBuilders.terms("cityAgg")  //?????????
                        .field("city")     //????????????
                        .size(100)           //??????????????????
        );

        searchRequest.source().aggregation(
                AggregationBuilders.terms("starAgg")  //?????????
                        .field("starName")     //????????????
                        .size(100)           //??????????????????
        );
    }

    private void buildBasicQuery(RequestParams params, SearchRequest searchRequest) {
        // ??????BooleanQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        // ???????????????
        String key = params.getKey();
        if (StringUtils.isEmpty(key)) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("all", key));
        }

        //????????????
        // ????????????
        if (!StringUtils.isEmpty(params.getCity())) {
            boolQuery.filter(QueryBuilders.termQuery("city", params.getCity()));
        }

        // ????????????
        if (!StringUtils.isEmpty(params.getBrand())) {
            boolQuery.filter(QueryBuilders.termQuery("brand", params.getBrand()));
        }

        // ????????????
        if (!StringUtils.isEmpty(params.getStarName())) {
            boolQuery.filter(QueryBuilders.termQuery("starName", params.getStarName()));
        }

        // ????????????
        if (!StringUtils.isEmpty(params.getMaxPrice()) && !StringUtils.isEmpty(params.getMinPrice())) {
            boolQuery.filter(QueryBuilders.rangeQuery("price")
                    .gte(params.getMinPrice())
                    .lte(params.getMaxPrice()));
        }

        // 2.????????????
        FunctionScoreQueryBuilder functionScoreQueryBuilder =
                QueryBuilders.functionScoreQuery(   //??????functionQuery
                        boolQuery,  // ???????????????????????????????????????
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{  // function score?????????
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(    // ???????????????function score ??????
                                        QueryBuilders.termQuery("isAD", true),  // ????????????
                                        ScoreFunctionBuilders.weightFactorFunction(100)  // ????????????
                                )
                        }
        );

        searchRequest.source().query(functionScoreQueryBuilder);

    }

    /**
     * ??????????????????
     *
     * @param search ??????????????????
     */
    private PageResult extracted(SearchResponse search) {
        //??????????????????
        //1.???????????????hits?????????
        SearchHits searchhits = search.getHits();

        //2.???????????????????????????
        long value = searchhits.getTotalHits().value;
        System.out.println("??????????????????:" + value);

        //3.??????????????????
        SearchHit[] hits = searchhits.getHits();

        ArrayList<HotelDoc> hotelDocs = new ArrayList<>();

        //4.??????
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();

            //????????????JSON
            HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);

            //???????????????
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length>0){
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }

            //??????????????????
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            //??????????????????
            HighlightField name = highlightFields.get("name");
            if (!StringUtils.isEmpty(name)) {  //???????????????????????????????????????
                //???????????????
                String string = name.getFragments()[0].string();    //name.getFragments()???Text??????????????????????????????????????????
                hotelDoc.setName(string);
            }

            hotelDocs.add(hotelDoc);

        }
        return new PageResult(value, hotelDocs);
    }

}
