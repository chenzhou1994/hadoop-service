package com.jeninfo.hadoopservice.client;

import com.jeninfo.hadoopservice.properties.ServiceProperties;
import com.jeninfo.hadoopservice.vo.Render;
import org.springframework.cloud.netflix.feign.FeignClient;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * @Author chenzhou
 * @Date 2019/3/10 12:39
 * @Description
 */
@Component
@FeignClient(value = ServiceProperties.HADOOP_SERVICE_PROVIDER)
public interface ProviderFeignClient {

    @RequestMapping(value = "/api/admin/user/select/all", method = RequestMethod.GET)
    Render selectAll(@RequestParam(value = "page", required = false, defaultValue = "1") Integer page,
                     @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize);
}
