package com.jeninfo.hadoopservice;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

/**
 * @author chenzhou
 * <p>
 * 通用返回类
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
@Accessors(chain = true)
public class Msg {
    /**
     * 状态码
     */
    private int code;

    /**
     * 是否成功
     */
    private boolean success;

    /**
     * 提示信息
     */
    private String msg;

    /**
     * 用户要返回给浏览器的数据
     */
    private Object data;

    public static Msg renderError() {
        Msg result = new Msg();
        result.setCode(200).setMsg("处理失败！").setSuccess(false);
        return result;
    }

    public static Msg renderSuccess(String msg, Integer code, Object object) {
        Msg result = new Msg();
        return result.setCode(code).setMsg(msg).setSuccess(true).setData(object);
    }
}
