package com.jeninfo.hadoopservice.client;

import com.jeninfo.hadoopservice.model.User;
import com.jeninfo.hadoopservice.vo.BaseController;
import com.jeninfo.hadoopservice.vo.Render;
import feign.hystrix.FallbackFactory;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * @author chenzhou
 * @date 2019/4/15 15:02
 * @description
 */
@Component
public class ProviderClientFallbackFactory implements FallbackFactory<ProviderFeignClient> {
    @Override
    public ProviderFeignClient create(Throwable cause) {
        return new ProviderFeignClient() {
            @Override
            public Render selectAll(Integer page, Integer pageSize) {
                return null;
            }

            @Override
            public Render select(String id) {
                return new BaseController().renderSuccess(new User().setId(UUID.randomUUID().toString()).setName("该ID" + id + "没有对应记录！服务暂停!").setPwd(""), 0x1111, "获取成功!");
            }
        };
    }
}
