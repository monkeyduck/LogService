package com.xiaole.redis;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.Map;

/**
 * Created by yuxiao on 16/9/4.
 */
public interface IKVStore {
    //代替redis执行redis的一些基本操作
    public void strHset(String key, String field, String value);
    public void objHset(String key, String field, Object value);

    public void strSet(String key, String value);
    public void objSet(String key, Object value);


    public String strGet(String key);

    public Object objGet(String key);

    public String strHget(String key, String field);

    void strHdel(String key, String field);
    void objHdel(String key, String field);
    Map<String,String> strHgetAll(String key);
    public Object objHget(String key, String field);

    public void strDel(String key);

    public void objDel(String key);

    public boolean exists(String key);

    public void strLpush(String key, String value);
    public void strLtrim(String key, long startIndex, long endIndex);
    public List<String> strLget(String key);
    void strLset(String key, long index, String value);

    public void expireAt(String key, long unixSeconds);

    public void setObjRedisTemplate(RedisTemplate<String, Object> redisTemplate) ;

    public void setStrRedisTemplate(StringRedisTemplate strtemplate);

    public void close();
}
