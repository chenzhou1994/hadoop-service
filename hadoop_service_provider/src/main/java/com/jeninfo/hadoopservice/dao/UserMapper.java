package com.jeninfo.hadoopservice.dao;

import com.jeninfo.hadoopservice.model.User;

import java.util.List;

/**
 * @author chenzhou
 */
public interface UserMapper {
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
    int insert(User record);

    /**
     * 根据主键查找
     *
     * @param id
     * @return
     */
    User selectByPrimaryKey(Integer id);

    /**
     * 获取所有
     *
     * @return
     */
    List<User> selectAll();

    /**
     * 更新
     *
     * @param record
     * @return
     */
    int updateByPrimaryKey(User record);
}