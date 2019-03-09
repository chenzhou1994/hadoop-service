package com.jeninfo.hadoopservice.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.jeninfo.hadoopservice.dao.UserMapper;
import com.jeninfo.hadoopservice.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author chenzhou
 */
@Service
public class UserService {
    @Autowired
    private UserMapper userMapper;

    /**
     * 分页获取
     *
     * @param page
     * @param pageSize
     * @return
     */
    public IPage<User> selectByPage(Integer page, Integer pageSize) {
        Page<User> userPage = new Page<>(page, pageSize);
        return userMapper.selectPage(userPage, null);
    }

    /**
     * 主键获取
     *
     * @param id
     * @return
     */
    public User selectById(String id) {
        return userMapper.selectById(id);
    }
}
