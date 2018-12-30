package com.jeninfo.hadoopservice.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * @author fancy
 * @date 2018-11-23 21:50
 */
@Accessors(chain = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RenderPage extends Render {

    private Integer page;

    private Integer pageSize;

    private Integer totalCount;
}
