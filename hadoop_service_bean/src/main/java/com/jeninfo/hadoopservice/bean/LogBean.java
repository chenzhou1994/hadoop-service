package com.jeninfo.hadoopservice.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * @Author chenzhou
 * @Date 2019/3/14 13:24
 * @Description
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
@Accessors(chain = true)
public class LogBean {

    /**
     * 记录客户端的ip地址
     */
    private String remote_addr;
    /**
     * 记录客户端用户名称,忽略属性"-"
     */
    private String remote_user;
    /**
     * 记录访问时间与时区
     */
    private String time_local;
    /**
     * 记录请求的url与http协议
     */
    private String request;
    /**
     * 记录请求状态；成功是200
     */
    private String status;
    /**
     * 记录发送给客户端文件主体内容大小
     */
    private String body_bytes_sent;

    /**
     * 用来记录从那个页面链接访问过来的
     */
    private String http_referer;

    /**
     * 记录客户浏览器的相关信息
     */
    private String http_user_agent;

    /**
     * 判断数据是否合法
     */
    private boolean valid = true;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.valid);
        sb.append("\001").append(this.remote_addr);
        sb.append("\001").append(this.remote_user);
        sb.append("\001").append(this.time_local);
        sb.append("\001").append(this.request);
        sb.append("\001").append(this.status);
        sb.append("\001").append(this.body_bytes_sent);
        sb.append("\001").append(this.http_referer);
        sb.append("\001").append(this.http_user_agent);

        return sb.toString();

    }
}
