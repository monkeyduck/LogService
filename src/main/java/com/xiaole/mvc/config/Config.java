package com.xiaole.mvc.config;

import com.xiaole.elasticsearch.ELServer;
import com.xiaole.hdfs.HDFSManager;
import com.xiaole.redis.IKVStore;
import com.xiaole.redis.RedisAdapter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by llc on 16/11/25.
 */
@Configuration
public class Config {
    @Bean(name="hdfsManager")
    public HDFSManager HDFSManager() {
        return new HDFSManager();
    }

    @Bean(name="redis")
    public IKVStore IKVStore(){
        return RedisAdapter.getInstance();
    }

    @Bean(name="elServer")
    public ELServer ELServer() {
        return new ELServer();
    }

}
