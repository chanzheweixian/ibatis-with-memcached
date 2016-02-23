在ibatis源码基础上修改，增加对memcached支持，通过配置IBatis的xml文件即可实现memcached细粒度话缓存，使用简单，缓存效果好。
spring下首先初始化MemcachedManager对象,或者通过程序初始化也一样,不要用ibatis官方的jar包,否则会冲突

<bean class="com.ibatis.sqlmap.engine.cache.memcached.memcachedManager" lazy-init="false"
> init-method="init" destroy-method="closePool">
> 

&lt;property name="serverlist"&gt;


> > 

&lt;value&gt;

192.168.0.1:11111, 192.168.0.2:11111

&lt;/value&gt;



> 

&lt;/property&gt;


> 

&lt;property name="initConn" value="5"&gt;



&lt;/property&gt;


> 

&lt;property name="minConn" value="5"&gt;



&lt;/property&gt;


> 

&lt;property name="maxConn" value="200"&gt;



&lt;/property&gt;




Unknown end tag for &lt;/bean&gt;



然后配置sqlMapConfig.xml文件:
<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE sqlMapConfig PUBLIC "-//iBATIS.com//DTD SQL Map Config 2.0//EN"
> "http://ibatis-with-memcached.googlecode.com/files/sql-map-config-2.dtd"> #注意这里,不用官方的,这个dtd文件加了个新属性databaseUrl,区分不同数据库的缓存对象



&lt;sqlMapConfig&gt;


> 

&lt;properties resource="cache\_config.properties"/&gt;


> <settings
> > cacheModelsEnabled="true"
> > enhancementEnabled="true"
> > lazyLoadingEnabled="true"
> > maxRequests="256"
> > maxSessions="256"
> > maxTransactions="150"
> > useStatementNamespaces="true"
> > databaseUrl="数据库名或地址" #新增加的属性
> > />
ibatis的xml文件:Albums.xml
#创建缓存model


&lt;cacheModel type="MEMCACHED" id="albumsCache"&gt;



> 

&lt;flushInterval hours="12"&gt;

</flushInterval >
> 

&lt;flushOnExecute statement="albums.save"&gt;

</flushOnExecute >
> 

&lt;flushOnExecute statement="albums.update"&gt;

</flushOnExecute >
> 

&lt;flushOnExecute statement="albums.delete"&gt;

</flushOnExecute >
> 

&lt;property name="pk" value="id"&gt;



&lt;/property&gt;

 #可以根据主键进行缓存,可以设置为空,不能不设
> 

&lt;property name="groupField" value="accId"&gt;



&lt;/property&gt;

 #可以根据组(比如用户id)进行缓存,更加细粒度化,可以设置为空,不能不设


Unknown end tag for &lt;/cacheModel&gt;



#加入缓存


&lt;select id="findall" parameterClass="albumsObj" resultClass="albumsObj" cacheModel="albumsCache"&gt;


> Select ……from albums where accId=1


&lt;/select&gt;




&lt;select id="load" parameterClass="albumsObj" resultClass="albumsObj" cacheModel="albumsCache"&gt;


> Select ……from albums where id=1


&lt;/select&gt;



#删除对象,删除缓存


&lt;delete id="delete" parameterClass="albumsObj"&gt;


> delete from albums where id=1 and accId=1 #(加上accId可以删除分组缓存)


&lt;/delete&gt;

