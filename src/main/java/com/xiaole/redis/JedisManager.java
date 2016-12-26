package com.xiaole.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.rmi.NoSuchObjectException;

/**
 * 本方法适合多线程访问redis数据库时候使用
 * 如果是单线程的话，直接起一个jedis instance 操作就可以了
 * 多线程时必须使用pool
 * 配置redis时 需要 在配置文件中 更改 bind 127.0.0.1  更改端口号  不要是 6379
 * @author yuxiao
 *
 */
public class JedisManager {

	private static JedisPool pool = null;
	//private static PLog pLog = PLogFactory.getLog(JedisManager.class.getName());
	static{
		init();
	}
	private JedisManager(){}
	private static synchronized void init(){

    	if(pool ==null){

            JedisPoolConfig config = new JedisPoolConfig();
            //控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
            //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
            config.setMaxTotal(-1);
            //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
            config.setMaxIdle(-1);
            //控制一个pool最少有多少个状态为idle(空闲的)的jedis实例。
            config.setMinIdle(2000);
            //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
            config.setMaxWaitMillis(1000);//做多等1s
            //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
            //config.setTestOnBorrow(true);
            //线程池用尽则block？
            config.setBlockWhenExhausted(true);
//            pool = new JedisPool(config, "101.200.206.78",6666, 1000, "xiaolelog");//目前仅绑定端口号和本地ip 就好了
			pool = new JedisPool(config, "101.201.82.247",6666, 1000, "xiaolelog");//目前仅绑定端口号和本地ip 就好了

//			pool = new JedisPool(config, "10.44.143.42", 6666);
           // pool = new JedisPool(config, "127.0.0.1", port, soTimeout, password, database, clientName)
    	}

	}


	/**
	 * 用来向线程池获取一个redis链接
	 * @return 一个redis 实例
	 * @throws NoSuchObjectException 获取失败则返回此异常  借来的jedis 可以直接调用 close方法结束
	 * 不必显示返回资源池
	 */
    public static Jedis getJedis() throws NoSuchObjectException{
    	if(pool==null){
    		init();
    	}
    	Jedis jedis = null;
    	try{
    		jedis = pool.getResource();
    	}catch(Exception e){
    		throw new NoSuchObjectException(e.getMessage());
    	}
    	return jedis;
    }


	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			Jedis js = JedisManager.getJedis();
//			DateTime dt = new DateTime();
//			long ts = dt.getMillis();
//			System.out.println(ts);
//			for (long i = ts - 10; i < ts; ++i){
//				js.set(""+i, ""+(ts - i));
//				js.expire(""+i, 1);//设置为1天吧
//			}
//			js.set("abc", "llc");
			System.out.println(js.getrange("stats-2016.11.24.16:16", 0,5));
//			String t = js.("stats-2016.11.24.16:16");
//			System.out.println("res is "+t);
			js.close();//这样就自动返回线程池了
		} catch (NoSuchObjectException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("no jedis returned");
		}

	}

}
