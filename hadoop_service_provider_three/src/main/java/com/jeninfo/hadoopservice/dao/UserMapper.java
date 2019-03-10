package com.jeninfo.hadoopservice.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.jeninfo.hadoopservice.model.User;
import org.springframework.stereotype.Component;

/**
 * @author chenzhou
 */
@Component
public interface UserMapper extends BaseMapper<User> {
}