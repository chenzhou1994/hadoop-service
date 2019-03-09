package com.jeninfo.hadoopservice.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.jeninfo.hadoopservice.model.User;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author chenzhou
 */
@Component
public interface UserMapper extends BaseMapper<User> {
}