//package com.jeninfo.hadoopservice.mvc;
//
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
//import org.springframework.test.context.web.WebAppConfiguration;
//import org.springframework.test.web.servlet.MockMvc;
//import org.springframework.test.web.servlet.MvcResult;
//import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
//import org.springframework.test.web.servlet.setup.MockMvcBuilders;
//import org.springframework.web.context.WebApplicationContext;
//
///**
// * @author chenzhou
// */
//@RunWith(SpringJUnit4ClassRunner.class)
//@WebAppConfiguration
//@SpringBootTest
//public class MVCTest {
//    // 传入Springmvc的ioc
//    @Autowired
//    private WebApplicationContext context;
//
//    // 虚拟mvc请求，获取到处理结果。
//    private MockMvc mockMvc;
//
//    @Before
//    public void initMokcMvc() {
//        mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
//    }
//
//
//    public void testPage() throws Exception {
//        //模拟请求拿到返回值
//        MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get("/topic/send")
//                .param("page", "1")
//                .param("pageSize", "10"))
//                .andReturn();
//        System.out.println(result.getRequest().getAttribute("user"));
//    }
//}
