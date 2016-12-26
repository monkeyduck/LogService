package com.xiaole.redis;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yuxiao on 16/9/4.
 */
public class RedisAdapter implements IKVStore {
    private static RedisAdapter rAdapter;
    RedisTemplate<String,Object> objRedisTemplate;
    StringRedisTemplate strRedisTemplate;

    public static void setrAdapter(RedisAdapter radp){
        rAdapter = radp;
    }

    public static IKVStore getInstance(){
        if(rAdapter==null){
//            FileSystemXmlApplicationContext ctx = new FileSystemXmlApplicationContext("classpath:spring-redis.xml");
           // setVersion(VersionManager.getVersion());
            ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("spring-redis.xml");
            rAdapter = (RedisAdapter) ctx.getBean("RedisAdapter");
            System.out.println("init rAdapter OK");

        }
        return rAdapter;
    }

    //代替redis执行redis的一些基本操作
    @Override
    public void strHset(String key,String field,String value){
        strRedisTemplate.opsForHash().put(key,field,value);
    }
    @Override
    public void objHset(String key,String field,Object value){
        objRedisTemplate.opsForHash().put(key,field,value);
    }
    @Override
    public void strSet(String key,String value){
        strRedisTemplate.opsForValue().set(key,value);
    }
    @Override
    public void objSet(String key,Object value){
        objRedisTemplate.opsForValue().set(key,value);
    }

    @Override
    public String strGet(String key){
        return strRedisTemplate.opsForValue().get(key);
    }
    @Override
    public Object objGet(String key){
        return objRedisTemplate.opsForValue().get(key);
    }
    @Override
    public String strHget(String key,String field){
        return (String) strRedisTemplate.opsForHash().get(key,field);
    }

    @Override
    public void strHdel(String key, String field) {
         strRedisTemplate.opsForHash().delete(key,field);
    }

    @Override
    public void objHdel(String key, String field) {
        objRedisTemplate.opsForHash().delete(key,field);
    }

    @Override
    public Map<String, String> strHgetAll(String key) {
         Map<Object,Object> fres = strRedisTemplate.opsForHash().entries(key);
        Map<String,String> map = new HashMap<String, String>();
        for(Map.Entry<Object,Object> entry : fres.entrySet()){
            map.put((String)entry.getKey(),(String)entry.getValue());
        }
        return map;
    }


    @Override
    public Object objHget(String key,String field){
        return  objRedisTemplate.opsForHash().get(key,field);
    }

    @Override
    public void strDel(String key){
        strRedisTemplate.delete(key);
    }
    @Override
    public void objDel(String key){
        objRedisTemplate.delete(key);
    }
    @Override
    public boolean exists(String key){
        //return objRedisTemplate.hasKey(key);
        return strRedisTemplate.hasKey(key);//两个都行 暂时先用这个吧
    }
    @Override
    public void strLpush(String key,String value){
        strRedisTemplate.opsForList().leftPush(key,value);
    }
    @Override
    public void strLtrim(String key,long startIndex,long endIndex){
        strRedisTemplate.opsForList().trim(key,startIndex,endIndex);
    }
    @Override
    public List<String> strLget(String key){
        return strRedisTemplate.opsForList().range(key,0,-1);
    }

    @Override
    public void strLset(String key, long index, String value) {
        strRedisTemplate.opsForList().set(key,index,value);
    }

    @Override
    public void expireAt(String key,long unixSeconds){
        strRedisTemplate.expireAt(key,new Date(unixSeconds*1000));
    }

    @Override
    public void setObjRedisTemplate(RedisTemplate<String, Object> redisTemplate) {
        this.objRedisTemplate = redisTemplate;
    }
    @Override
    public void setStrRedisTemplate(StringRedisTemplate strtemplate){
        this.strRedisTemplate = strtemplate;
    }
    @Override
    public void close(){

    }


    public static void main(String[] args){
        IKVStore ra = RedisAdapter.getInstance();
        String key = "test";

        ra.strHset(key,"k1","v1");
        ra.strHset(key,"k2","v2");
        ra.strHset(key,"k3","v3");
        ra.strHset(key,"k4","v4");
        ra.strHdel(key,"k2");


        Map<String,String> res = ra.strHgetAll(key);
        for(Map.Entry<String,String> entry :res.entrySet()){
            System.out.println(entry.toString());
        }

        System.out.println(ra.exists(key));
    }
}
