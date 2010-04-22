package com.ibatis.sqlmap.engine.cache.memcached;

import java.io.Serializable;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import com.ibatis.common.logging.Log;
import com.ibatis.common.logging.LogFactory;

import com.danga.MemCached.MemCachedClient;
import com.danga.MemCached.SockIOPool;

/**
 * Memcached manager
 * 
 * @author <a href="mailto:fmlou@163.com">HongzeZhang</a>
 * 
 * @version 1.0
 * 
 * @since Oct 23, 2007
 */
public class MemcachedManager {

	private static Log logger = LogFactory.getLog(MemcachedManager.class);

	private static String memcachedDomain = "IBATIS_CACHED"; // memcached 域名

	private String serverlist;

	private static MemCachedClient mcc = null;

	private SockIOPool pool = null;

	private int initConn = 5;

	private int minConn = 5;

	private int maxConn = 50;

	/**
	 * 初始化连接池
	 */
	public void init() {
		if (mcc != null)
			return;

		logger.debug("Initializing ibatis memcached manager start.");
		if (pool == null) {
			try {
				pool = SockIOPool.getInstance(memcachedDomain);
				pool.setServers(serverlist.split(","));
				if (!pool.isInitialized()) {
					pool.setInitConn(initConn);
					pool.setMinConn(minConn);
					pool.setMaxConn(maxConn);
					pool.setMaintSleep(30);
					pool.setNagle(false);
					pool.setSocketTO(60 * 60);
					pool.setSocketConnectTO(0);
					pool.setHashingAlg(SockIOPool.CONSISTENT_HASH);
					pool.initialize();
				}
			} catch (Exception ex) {
				logger.error(ex.getMessage());
			}
		}

		if (mcc == null) {
			mcc = new MemCachedClient(memcachedDomain);
			mcc.setCompressEnable(false);
			mcc.setCompressThreshold(0);
		}
		logger.debug("Initializing ibatis memcached manager ok!");
	}

	/**
	 * 关闭连接池
	 */
	public void closePool() {
		pool.shutDown();
		mcc = null;
		pool = null;
		logger.debug("Ibatis memcached manager closed");
	}

	/**
	 * 设置缓存
	 * 
	 * @param key
	 *            键
	 * @param obj
	 *            值
	 */
	public static boolean set(Object key, Serializable obj) {
		logger.debug("set key:" + getKey(key) + ", value:" + obj);
		try {
			return mcc.set(getKey(key), obj);
		} catch (Exception e) {
			logger.error("Pool set error!");
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 设置缓存
	 * 
	 * @param key
	 *            键
	 * @param obj
	 *            值
	 * @param cacheTime
	 *            缓存时间（毫秒）
	 */
	public static boolean set(Object key, Serializable obj, long cacheTime) {
		try {
			return mcc.set(getKey(key), obj, new Date(cacheTime));
		} catch (Exception e) {
			logger.error("Pool set error!");
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 替换缓存
	 * @param key 键
	 * @param value 值
	 * @param cacheTime 缓存时间(毫秒)
	 */
	public static void replace(int key, Serializable value, long cacheTime) {
		try {
			mcc.replace(getKey(key), value, new Date(cacheTime));
		} catch (Exception e) {
			logger.error(" pool set error!");
		}
	}

	/**
	 * 获取缓存
	 * 
	 * @param key
	 *            键
	 * @return 值
	 */
	public static Object get(Object key) {
		Object result = null;
		String realkey = getKey(key);
		try {
			result = mcc.get(realkey);
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.debug("get key:" + getKey(key) + ", value:" + result);
		return result;
	}

	/**
	 * 缓存数值
	 * 
	 * @param key
	 *            键
	 * @param count
	 *            数值
	 */
	public static void setCounter(Object key, long count) {
		try {
			mcc.storeCounter(getCountKey(key), count);
		} catch (Exception e) {
			logger.error("Pool setCounter error!");
		}
	}

	/**
	 * 缓存的数值加1
	 * 
	 * @param key
	 *            键
	 */
	public static void addCounter(Object key) {
		if(mcc.get(getCountKey(key))==null){
			mcc.storeCounter(getCountKey(key), 0);
		}
		try {
			mcc.incr(getCountKey(key));
		} catch (Exception e) {
			logger.error("Pool setCounter error!");
		}
	}

	/**
	 * 缓存的数值减1
	 * 
	 * @param key
	 *            键
	 */
	public static void decreaseCounter(Object key) {
		try {
			mcc.decr(getCountKey(key));
		} catch (Exception e) {
			logger.error("Pool setCounter error!");
		}
	}

	/**
	 * 增加缓存数值
	 * 
	 * @param key
	 *            键
	 * @param addValue
	 *            增加的值
	 */
	public static void addCounter(Object key, long addValue) {
		try {
			mcc.incr(getCountKey(key), addValue);
		} catch (Exception e) {
			logger.error(" pool setCounter error!");
		}
	}

	/**
	 * 获取缓存数值
	 * 
	 * @param key
	 *            键
	 * @return 值
	 */
	public static long getCounter(Object key) {
		long result = 0;
		try {
			result = mcc.getCounter(getCountKey(key));
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		return result;
	}

	/**
	 * 删除缓存
	 * 
	 * @param key
	 *            键
	 */
	public static boolean delete(Object key) {
		try {
			return mcc.delete(getKey(key));
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		return false;
	}

	/**
	 * 删除缓存数值
	 * 
	 * @param key
	 *            键
	 */
	public static long deleteCounter(Object key) {
		try {
			return mcc.decr(getCountKey(key));
		} catch (Exception e) {
			logger.error(" pool setCounter error!");
		}
		return 0; 
	}

	/**
	 * 清空所有缓存(慎用)
	 */
	public static void flushAll() {
		mcc.flushAll();
	}
	
	@SuppressWarnings("unchecked")
	public static long size(){
		long size=0L;
		Map<String,Map<String,String>> status=mcc.statsItems();
		Collection<Map<String,String>> values=status.values();
		for (Map<String,String> state:values) {
			String num=state.get("items:1:number");
			if(num==null)
				continue;
			
			size+=Long.parseLong(state.get("items:1:number"));
		}
		return size;
	}

	/**
	 * 创建键值
	 * 
	 * @param key
	 * @return
	 */
	private static String getKey(Object key) {
		return memcachedDomain + "@" + key;
	}

	private static String getCountKey(Object key) {
		return memcachedDomain + "@" + key + "_count";
	}

	/**
	 * 设置服务器地址,以逗号隔开 
	 * @param serverlist 服务器地址
	 */
	public void setServerlist(String serverlist) {
		this.serverlist = serverlist;
	}

	/**
	 * 设置初始化连接数
	 * @param initConn 初始化连接数
	 */
	public void setInitConn(int initConn) {
		this.initConn = initConn;
	}

	/**
	 * 设置最小连接数
	 * @param minConn 最小连接数
	 */
	public void setMinConn(int minConn) {
		this.minConn = minConn;
	}

	/**
	 * 设置最大连接数
	 * @param maxConn 最大连接数
	 */
	public void setMaxConn(int maxConn) {
		this.maxConn = maxConn;
	}

	/**
	 * 设置cache域
	 * @param memcachedKey 域名
	 */
	public void setMemcachedDomain(String memcachedKey) {
		MemcachedManager.memcachedDomain = memcachedKey;
	}
}

