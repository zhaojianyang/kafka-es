package com.allen.demo.controller;


import com.alibaba.fastjson.JSONObject;
import com.allen.demo.response.Result;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.validation.constraints.NotNull;
import java.util.Date;


/**
 * 设备资源管理接口
 *
 * @author qisong.li@bitmain.com
 * @version 1.0.0
 * @since 2019/03/11
 */
@RequestMapping("/test")
@RestController
@Slf4j
public class TestController {


    @Resource
    private RestHighLevelClient esClient;

    /**
     * 测试：
     */
    @CrossOrigin
    @RequestMapping(value = "/es", method = RequestMethod.GET)
    @ResponseBody
//    public Result deleteFile(@RequestParam("group") @Validated @NotNull String group, @RequestParam("path") @Validated @NotNull String path)  {
    public Result addToEs()  {

        BulkRequest bulkRequest = new BulkRequest();


        ObjectMapper mapper = new ObjectMapper();
        JSONObject json = new JSONObject();
        json.put("name","testJson");
        json.put("age","1");
        json.put("country","china");
        json.put("date",new Date());


        try {
            bulkRequest.add(new IndexRequest("test_json_" + 10, "person").source(mapper.writeValueAsBytes(json), XContentType.JSON));
        } catch (Exception e) {
            log.error("bulkRequest.add test_json error "+e.getMessage(),e);

            e.printStackTrace();
        }

        try {
            BulkResponse r = esClient.bulk(bulkRequest);
            if(r.hasFailures()){
                log.error("add  test_json:"+r.buildFailureMessage());

                return Result.error(r.buildFailureMessage());
            }
        } catch (Exception e) {
            log.error("add test_json error "+e.getMessage(),e);

            e.printStackTrace();
        }
        log.info("add test_json ok");



        return Result.ok();
    }


}
