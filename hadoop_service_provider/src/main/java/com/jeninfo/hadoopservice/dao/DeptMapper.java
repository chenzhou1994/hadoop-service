package com.jeninfo.hadoopservice.dao;

import com.jeninfo.hadoopservice.model.Dept;

import java.util.List;

/**
 * @author chenzhou
 */
public interface DeptMapper {
    /**
     * 删除
     *
     * @param id
     * @return
     */
    int deleteByPrimaryKey(Integer id);

    /**
     * 新增
     *
     * @param record
     * @return
     */
    int insert(Dept record);

    /**
     * 根据主键查找
     *
     * @param id
     * @return
     */
    Dept selectByPrimaryKey(Integer id);

    /**
     * 获取所有
     *
     * @return
     */
    List<Dept> selectAll();

    /**
     * 更新
     *
     * @param record
     * @return
     */
    int updateByPrimaryKey(Dept record);
}