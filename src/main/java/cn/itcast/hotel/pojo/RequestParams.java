package cn.itcast.hotel.pojo;

import lombok.Data;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: fxiao
 * @Version: 2022/05/12/23:30
 */
@Data
public class RequestParams {
    private String key;
    private Integer page;
    private Integer size;
    private String sortBy;
    private String city;
    private String brand;
    private String starName;
    private Integer minPrice;
    private Integer maxPrice;
    private String location;

}
