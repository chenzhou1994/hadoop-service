package com.jeninfo.hadoopservice.service;

import com.github.pagehelper.PageHelper;
import com.jeninfo.hadoopservice.dao.UserMapper;
import com.jeninfo.hadoopservice.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author chenzhou
 */
@Service
public class UserService {
    @Autowired
    private UserMapper userMapper;

    public List<User> selectAll(Integer page, Integer pageSize) {
        return PageHelper.startPage(page, pageSize).doSelectPage(() -> userMapper.selectAll());
    }

    public User selectById(String id) {
        return userMapper.selectByPrimaryKey(Integer.parseInt(id));
    }
}
