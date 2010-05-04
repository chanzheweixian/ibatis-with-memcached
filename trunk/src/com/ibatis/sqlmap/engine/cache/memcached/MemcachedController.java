package com.ibatis.sqlmap.engine.cache.memcached;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.ibatis.common.logging.Log;
import com.ibatis.common.logging.LogFactory;
import com.ibatis.sqlmap.engine.cache.CacheController;
import com.ibatis.sqlmap.engine.cache.CacheModel;

/**
 * Cache implementation for using Memcached with iBATIS
 * 
 * @author <a href="mailto:fmlou@163.com">HongzeZhang</a>
 * 
 * @version 1.0
 * 
 *          2009-5-6
 */
public class MemcachedController implements CacheController {

	private static final Log log = LogFactory.getLog(MemcachedController.class);

	private static final Object NULL_VALUE = "SERIALIZABLE_NULL_OBJECT";

	// 主键
	public String pk;

	// 缓存分组字段
	public String groupField;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ibatis.sqlmap.engine.cache.CacheController#flush(com.ibatis.sqlmap
	 * .engine.cache.CacheModel)
	 */
	@SuppressWarnings("unchecked")
	public void flush(CacheModel cacheModel) {
		String groupName = cacheModel.getId();
		String groupkey = getGroupKey(cacheModel, groupName);
		log.debug("--------------------------------------flush:" + groupkey);
		HashSet<String> group = (HashSet<String>) MemcachedManager.get(groupkey);
		if (group != null && !group.isEmpty()) {
			MemcachedManager.set(groupkey, new HashSet<String>());
			for (String k : group) {
				MemcachedManager.delete(k);
			}
		}
	}

	public void flush(CacheModel cacheModel, Object key) {
		log.debug("----flush key:" + key);
		String groupName = cacheModel.getId();
		String groupkey = null;
		String userId = null;
		if ((userId = this.getGroupIdValue(key)) != null) {
			groupkey = getUserGroupKey(cacheModel, groupName, userId);
			deleteGroup(groupkey);
		}
		groupkey = getGroupKey(cacheModel, groupName);
		deleteGroup(groupkey);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ibatis.sqlmap.engine.cache.CacheController#getObject(com.ibatis.sqlmap
	 * .engine.cache.CacheModel, java.lang.Object)
	 */
	public Object getObject(CacheModel cacheModel, Object key) {
		log.debug("----sqlKey:" + key);
		Object result = null;
		String k = null;
		if (this.isSqlFromPK(key) && (k = this.getBeanKey(cacheModel,key)) != null) {
			log.debug("----getBean key:" + k);
			result = MemcachedManager.get(k);
		} else {
			k = getCollectionKey(cacheModel,key);
			log.debug("----getCollection key:" + k);
			result = MemcachedManager.get(k);
		}

		if (result != null && result.equals(NULL_VALUE)) {
			log.debug("----CacheObject null!");
			return null;
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ibatis.sqlmap.engine.cache.CacheController#putObject(com.ibatis.sqlmap
	 * .engine.cache.CacheModel, java.lang.Object, java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	public void putObject(CacheModel cacheModel, Object key, Object object) {
		// System.out.println("key=" + key);
		String k = null;

		// 检查是否是根据主键查询
		if (this.isSqlFromPK(key) && (k = this.getBeanKey(cacheModel, key)) != null) {
			// 获取表名和主键值
			MemcachedManager.set(k, (Serializable) object, cacheModel.getFlushInterval());

			log.debug("----putObject key:" + k + ",value:" + object);
		} else {
			String groupName = cacheModel.getId();
			String groupkey = null;
			String userId = null;
			k = getCollectionKey(cacheModel,key);
			if ((userId = this.getGroupIdValue(key)) != null) {
				groupkey = getUserGroupKey(cacheModel,groupName, userId);
			} else {
				groupkey = getGroupKey(cacheModel,groupName);
			}
			HashSet<String> group = (HashSet<String>) MemcachedManager
					.get(groupkey);
			if (group == null)
				group = new HashSet<String>();
			group.add(k);
			MemcachedManager.set(groupkey, group, cacheModel.getFlushInterval());
			MemcachedManager.set(k, (Serializable) object, cacheModel.getFlushInterval());
			log.debug("----putCollection key:" + k + ",value:" + object
					+ ",group:" + groupName);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ibatis.sqlmap.engine.cache.CacheController#removeObject(com.ibatis
	 * .sqlmap.engine.cache.CacheModel, java.lang.Object)
	 */
	public Object removeObject(CacheModel cacheModel, Object key) {
		String k = null;
		if (this.isSqlFromPK(key) && (k = this.getBeanKey(cacheModel, key)) != null) {
			log.debug("----removeObject key:" + k);
			MemcachedManager.delete(k);
			return null;
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ibatis.sqlmap.engine.cache.CacheController#setProperties(java.util
	 * .Properties)
	 */
	public void setProperties(Properties props) {
		// 获取表的键值
		pk = (String) props.get("pk");

		// 缓存分组字段名
		groupField = (String) props.get("groupField");
	}

	// 获取结果集为集合的键值
	private String getCollectionKey(CacheModel cacheModel, Object key) {
		log.debug("---database url :"+cacheModel.getDatabaseUrl());
		String k = key.toString();
		Pattern p = Pattern.compile("\\-?\\d*\\|\\-?\\d*\\|(.*)");
		Matcher m = p.matcher(k);
		if (m.find(1)) {
			k = m.group(1);
		}
		k = k.replaceFirst("\\|\\d*\\|\\s", "");
		k = cacheModel.getDatabaseUrl() + k;
		log.debug("----getCollectionKey:" + k);
		return Md5Util.getMD5Str(k);
	}

	// 获取结果集为单个对象的键值
	private String getBeanKey(CacheModel cacheModel, Object key) {
		log.debug("---database url :"+cacheModel.getDatabaseUrl());
		String table = this.getTableFromSql(key);
		String pkValue = this.getPkValue(key);
		if (table == null || pkValue == null)
			return null;
		return cacheModel.getDatabaseUrl() + "@" + table + "_" + pkValue;
	}

	// 获取sql中的表名
	private String getTableFromSql(Object sqlKey) {
		Pattern p = Pattern
				.compile(".*\\s(from|From|update|UPDATE|delete|DELETE)\\s+(.+?)\\s.*");
		Matcher m = p.matcher(sqlKey.toString());
		if (m.find(2))
			return m.group(2);
		return null;
	}

	// 获取组键
	public String getGroupKey(CacheModel cacheModel, String key) {
		log.debug("---database url :"+cacheModel.getDatabaseUrl());
		if(this.groupField!=null)
			key=cacheModel.getId();
		log.debug("----getGroupKey:" + (cacheModel.getDatabaseUrl() + key));
		return cacheModel.getDatabaseUrl() + key;
	}

	// 判断是否根据主键查询
	public boolean isSqlFromPK(Object sqlKey) {
		if(pk==null || pk.trim().equals("") || sqlKey.toString().contains(" count(") || sqlKey.toString().contains(" COUNT("))
			return false;
		
		boolean flag = sqlKey.toString().matches(
				".*\\s(where|WHERE)(.*|.+?)\\s+" + pk + "\\s*=.*");
		log.debug("----isSqlFromPK:" + flag);
		return flag;
	}

	// 获取sql中的主键值
	public String getPkValue(Object sqlKey) {
		String key = sqlKey.toString();
		int start = key.indexOf("update");
		start = (start < 0) ? key.indexOf("UPDATE") : start;
		start = (start < 0) ? key.indexOf("where") : start;
		start = (start < 0) ? key.indexOf("WHERE") : start;
		if(start<0)
			return null;
		
		int end = key.indexOf(" " + pk, start);
		log.debug("----getPkValue: end:"+end);
		if (end < 0)
			return null;

		int n = StringUtils.countMatches(key.substring(start, end), "?");
		StringBuilder sb = new StringBuilder("\\-?\\d*\\|\\-?\\d*\\|");
		for (int i = 0; i < n; i++) {
			sb.append("(.|\\n|\\r)+?\\|");
		}
		sb.append("((.|\\n|\\r)+?)\\|.*");
		Pattern p = Pattern.compile(sb.toString());
		Matcher m = p.matcher(key);
		if (m.find(n+1)) {
			log.debug("pk:" + m.group(n+1));
			return m.group(n+1);
		}
		return null;
	}

	// 获取用户id
	public String getGroupIdValue(Object sqlKey) {
		if (groupField == null)
			return null;

		log.debug("sqlKey:" + sqlKey);
		String key = sqlKey.toString();

		if (key.matches("(.|\\n|\\r)*\\s(where|WHERE).*\\s+" + groupField + "\\s*=.*")) {

			int start = key.indexOf("update");
			start = (start < 0) ? key.indexOf("UPDATE") : start;
			start = (start < 0) ? key.indexOf("where") : start;
			start = (start < 0) ? key.indexOf("WHERE") : start;
			int end = key.indexOf(groupField, start);
			if (end < 0)
				return null;

			String s = key.substring(start, end);
			log.debug("sql=" + s);
			int n = StringUtils.countMatches(s, "?");
			log.debug("n=" + n);
			StringBuilder sb = new StringBuilder("\\-?\\d*\\|\\-?\\d*\\|");
			for (int i = 0; i < n; i++) {
				sb.append("(.|\\n|\\r)+?\\|");
			}
			sb.append("(.+?)\\|.*");
			log.debug("p=" + sb.toString());
			Pattern p = Pattern.compile(sb.toString());
			Matcher m = p.matcher(key);
			if (m.find(n+1)) {
				log.debug("userId:" + m.group(n+1));
				return m.group(n+1);
			}
		} else if (key.matches("(.|\\n|\\r)*(insert|INSERT)\\s+.*\\(.*,?\\s*"
				+ groupField + "\\s*,?.*\\)\\s*(values|values).*")) {
			int start = key.indexOf("insert");
			start = (start == 0) ? key.indexOf("INSERT") : start;
			log.debug("start="+start);
			int end = key.indexOf(groupField, start);
			log.debug("end="+end);
			if (end < 0)
				return null;

			String s = key.substring(start, end);
			log.debug("sql=" + s);
			int n = StringUtils.countMatches(s, ",");
			log.debug("n=" + n);
			StringBuilder sb = new StringBuilder("\\-?\\d*\\|\\-?\\d*\\|");
			for (int i = 0; i < n; i++) {
				sb.append("(.|\\n|\\r)+?\\|");
			}
			sb.append("((.|\\n|\\r)+?)\\|.*");
			log.debug("p=" + sb.toString());
			Pattern p = Pattern.compile(sb.toString());
			Matcher m = p.matcher(key);
			if (m.find(n+1)) {
				log.debug("userId:" + m.group(n+1));
				return m.group(n+1);
			}
		}

		return null;
	}

	// 获取用户集合组键
	private String getUserGroupKey(CacheModel cacheModel, Object key, String groupId) {
		if(this.groupField!=null)
			key=cacheModel.getId();
		
		log.debug("----getUserGroupKey:" + (cacheModel.getDatabaseUrl() + groupId + "@" + key));
		return cacheModel.getDatabaseUrl() + groupId + "@" + key;
	}

	// 删除缓存组
	@SuppressWarnings("unchecked")
	private void deleteGroup(String groupkey) {
		log.debug("----flush group:" + groupkey);
		HashSet<String> group = (HashSet<String>) MemcachedManager.get(groupkey);
		if (group != null && !group.isEmpty()) {
			MemcachedManager.set(groupkey, new HashSet<String>());
			for (String k : group) {
				log.debug("----delete key:" + k);
				MemcachedManager.delete(k);
			}
		}
	}
}
