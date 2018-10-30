package com.jeninfo.hadoopservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Date;

/**
 * @author chenzhou
 * @date 2018/10/30 20:39
 * @description
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
@Accessors(chain = true)
public class Article {
    private Integer id;
    private String title;
    private String content;
    private String url;
    private Date pubdate;
    private String source;
    private String author;
}
