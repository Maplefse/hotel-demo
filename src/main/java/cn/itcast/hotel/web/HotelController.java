package cn.itcast.hotel.web;

import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: fxiao
 * @Version: 2022/05/12/23:37
 */
@RestController
@RequestMapping("/hotel")
public class HotelController {

    @Autowired
    private IHotelService hotelService;

    @PostMapping("/list")
    public PageResult search(@RequestBody RequestParams params){
        return  hotelService.search(params);
    }

    @PostMapping("filters")
    public Map<String, List<String>> filters(@RequestBody RequestParams params){
        return hotelService.filters(params);
    }


}
