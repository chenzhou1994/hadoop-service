package com.jeninfo.hadoopservice.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * @author fancy
 * @date 2018-11-23 21:35
 */
@Accessors(chain = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Render {

    private Integer code;

    private Boolean success;

    private String message;

    private Object data;

}
