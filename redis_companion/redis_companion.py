from redis import Redis
from datetime import datetime

# 连接redis
r = Redis('127.0.0.1')

r.flushdb()  # 清除库
# r.flushall()  # 清除所有库

####################################################### 字符串kv对操作 #########################################
r.set('s1', '123')  # 设置字符串键值对
# set参数 set(self, name, value, ex=None, px=None, nx=False, xx=False)
# ex 设置过期时间,秒为单位
# px 设置过期时间,毫秒为单位
# nx 当键不存在的时候才能set
# xx 当键存在的时候才能set
r.append('s1', '4')  # 向字符串右边追加字符串
r.getrange('s1', 0, -1)  # 索引字符串,和python不同,他的范围是闭合区间[start,end],python是左闭右开[start,end)
r.setrange('s1', 1, 'zz')  # 从索引处开始覆盖字符串,返回覆盖操作后的字符串长度,如果key不存在则操作失败,并不会创建新的

print(r.get('s1'))  # 获取key的值,返回二进制
print(r.mget(['s1', 's2']))  # 一次查多个,返回列表,如果该key不存在则返回None,返回二进制
print(r.getset('s1', 'zxcvb'))  # 先查,再设置新值,如果没有该值就直接创建该值,返回二进制
print(r.strlen('s1'))  # 返回字符长度

r.mset({'s2': 'qwe', 's3': 'asd'})  # 设置多个键值对,原子操作,如果存在则覆盖
r.msetnx({'s3': 'qwe', 's4': 'asd'})  # 设置多个键值对,原子操作,如果存在则不进行设置

r.set('s5', 1)
r.incr('s5')  # 自增1(必须是数字)
r.incrby('s5', 5)  # 带步长的自增
r.decr('s5')  # 自减1
r.decrby('s5')  # 带步长的自减

r.set('s6', '中文')  # 中文默认utf-8
print(r.get('s6'))  # 一个字占3位

####################################################### 生存时间设定 #########################################
r.expire('s1', 1000)  # 设置过期时间,秒为单位
r.pexpire('s2', 10000)  # 设置过期时间,毫秒为单位
r.expireat('s3', round(datetime.now().timestamp()) + 1000)  # 设置过期时间,输入时间戳,秒为单位
r.persist('s3')  # 取消过期

print(r.ttl('s1'))  # 查询生存时间,秒为单位
print(r.pttl('s2'))  # 查询生存时间,毫秒为单位
# 返回-1   为key存在但没有设置生存时间
# 返回数值  为key的剩余生存时间
# 返回-2   为key曾经存在,但现在已经消亡(老版本返回-1)

####################################################### 显示所有key #########################################
print(r.keys())  # 显示所有key
# keys 参数 keys(self, pattern='*')
# pattern默认为'*',类似正则表达式
# '*' 匹配任意长度字符
# '?' 匹配一个任意字符
# ['字符'] 匹配括号中的任意一个字符

####################################################### 位图bitmap操作 #########################################
# 位图的本质还是字符串,并不是一种独立的数据类型,只是对字符串进行位操作
# 字符串最大512mb上限,为2^32位
r.set('s9', 9)  # 数字9在redis中会存储为字符串,9的ASCII码为57,57转为二进制是0b0011 1001,索引是从左到右并从0开始
print(r.getbit('s9', 0))  # 读取位0
print(r.getbit('s9', 1))
print(r.getbit('s9', 2))
print(r.getbit('s9', 3))
print(r.getbit('s9', 4))
print(r.getbit('s9', 5))
print(r.getbit('s9', 6))
print(r.getbit('s9', 7))
r.setbit('s9', 7, 0)  # 设置位7为0
print(r.getbit('s9', 7))

print(r.bitcount('s9'))  # 计算一共出现了多少次1
print(r.bitpos('s9', 1))  # 返回指定区间第一次出现1的位置
# bitpos参数 bitpos(self, key, bit, start=None, end=None):

r.set('s8', 7)  # 7的ascii码是55二进制为0b0011 0111和0b0011 1001
r.bitop('and', 's10', 's8', 's9')  # 位与,将s8和s9位与的结果给s10
print(r.get('s10'))  # 0b0011 0111和0b0011 1001位与结果为0b0011 0001,转十进制为49,对应ascii码为1
r.bitop('or', 's11', 's8', 's9')  # 位或
r.bitop('xor', 's12', 's8', 's9')  # 位异或
r.bitop('not', 's13', 's9')  # 位非
# bitop 可以对两个以上的kv对进行操作(not只能一个),如果长度不一致,较短的一方高位补0
print([bin(ord(i)) for i in r.mget(['s11', 's12', 's13'])])

####################################################### 列表list操作 #########################################
r.rpush('l1', 1, 2, 3, 4, 5)  # 向队列左边push元素
r.lpush('l1', 1, 2, 3, 4, 5)  # 向队列右边push元素
r.rpushx('l1', 6)  # 向队列右边push一个元素,key必须存在,key不存在则失败
r.lpushx('l1', 6)  # 向队列左边push一个元素,key必须存在,key不存在则失败
print(r.lrange('l1', 0, -1))  # 根据索引读取队列

print(r.rpop('l1'))  # 弹出右边第一个元素
print(r.lpop('l1'))  # 弹出左边第一个元素
print(r.rpoplpush('l1', 'l2'))  # l1右边弹出元素,并从左压入t2

print(r.lindex('l2', 0))  # 取索引的值
r.lset('l2', 0, '123')  # 根据索引设置值,index不能超界
r.lrem('l1', 2, 1)  # 从左到右弹出2个1
# lrem 参数 lrem(self, name, count, value)
# count 为正数则从左到右弹出,为负数则从右到左弹出
# 返回弹出个数

r.ltrim('l1', 0, 5)  # 保留索引0到5内的元素,其他的都去除

print(r.lrange('l1', 0, -1))
r.linsert('l1', 'before', 4, 999)  # 在元素值为4的元素之前(左边)插入999,有before和after可选,如果没有这个key或者元素则不进行操作
print(r.lrange('l1', 0, -1))

####################################################### 列表阻塞操作 #########################################
# 可以当低可用消息队列来使用
r.rpush('bl1', 1, 2)
print(r.blpop('bl1'))  # 阻塞性的从左边弹出元素
print(r.brpop('bl1'))  # 阻塞性的从右边弹出元素
print(r.blpop('bl1', timeout=1))  # 可以设置超时,超时则返回None,不抛异常
print(r.brpoplpush('bl1', 'bl2', timeout=1))  # 从bl1的右边弹出,从bl2的左边压入,超时则为None(bl2不压入元素)
print(r.lrange('bl2', 0, -1))

####################################################### 散列Hash类型 #########################################
# redis中的散列类型类似python的字典套字典{key:{k1:v1,k2:v2}}
r.hset('h1', 't1', 1)  # 设置字典值
r.hsetnx('h1', 't1', 2)  # 设置字典值,要求字段不存在,若存在则不更改
r.hsetnx('h1', 't2', 2)

print(r.hgetall('h1'))  # 取所有键值对,返回一个dict
print(r.hget('h1', 't1'))  # 取一个键值对
print(r.hmget('h1', 't1', 't2'))  # 取多个指定键值对的值,返回一个列表(和指定的键值顺序相同)

print(r.hexists('h1', 't3'))  # 判断键值是否存在,返回布尔值
print(r.hlen('h1'))  # 获得字典的键值对个数
print(r.hkeys('h1'))  # 获取字典所有的键,返回列表
print(r.hvals('h1'))  # 获取字典的所有值,返回列表

r.hincrby('h1', 't1', 2)  # h1字典的t1字段增加2(值只能是整形)
r.hincrbyfloat('h1', 't2', 0.5)  # h1字典的t2字段增加0.5(整形或浮点)
r.hdel('h1', 't1')  # 删除h1字典的t1字段

####################################################### 集合set操作 #########################################
r.sadd('set1', 1, 2, 3, 4, 5)  # 添加元素,如果存在则忽略
r.srem('set1', 5, 6, 7)  # 移除set的值,如果不存在则忽略
print(r.scard('set1'))  # 计算集合中的元素个数(O(1))
print(r.smembers('set1'), type(r.smembers('set1')))  # 返回集合中的所有元素,返回一个set,当元素数量很多时将花费大量IO时间
print(r.sismember('set1', 1))  # 查询元素是否在集合中,返回布尔值

print(r.srandmember('set1', number=2))  # 随机返回一定数量的元素,返回一个list
# srandmember 参数 srandmember(self, name, number=None)
# 当number为正数时,返回number个随机元素(不会重复),如果大于集合长度则返回整个集合
# 当number为负数是,返回abs(number)个随机元素(元素可能重复)
# 当number为0时,不返回元素
# 当number缺省时,随机返回一个

print(r.spop('set1'))  # 随机移除一个元素
print(r.smove('set1', 'set2', 1))  # 移动元素,将元素1移除set1并加入到set2中,返回布尔值

####################################################### 集合运算 #########################################
r.sadd('set3', 1, 2, 3, 4, 5)
r.sadd('set4', 5, 6, 7, 8, 9)
r.sadd('set5', 4, 5, 6, 7)

#### 求差集
print(r.sdiff('set3', 'set4', 'set5'))  # 求差集,set3减set4和set5
r.sdiffstore('set6', 'set3', 'set4', 'set5')  # 求差集并将结果赋值给set6
print(r.smembers('set6'))

### 求交集
print(r.sinter('set3', 'set4'))  # 求交集
r.sinterstore('set7', 'set3', 'set4')  # 求交集并赋值

### 求并集
print(r.sunion('set3', 'set4'))  # 求并集
r.sunionstore('set8', 'set3', 'set4')  # 求并集并赋值

####################################################### 有序集合SortedSet操作 #########################################
r.zadd('z1', {'k1': 1, 'k2': 2, 'k3': 3, 'k4': 4, 'k5': 1})  # 添加元素,如果元素已存在则覆盖  {key:score} score为浮点数
print(r.zincrby('z1', 1, 'k1'))  # 增加指定元素的score,值可为负数

print(r.zscore('z1', 'k1'))  # 返回指定元素的score
print(r.zcard('z1'))  # 返回元素个数
print(r.zcount('z1', 2, 3))  # 返回指定score区间的元素个数,闭区间

print(r.zrange('z1', 0, -1, desc=True, withscores=True))  # 返回指定区间的元素,闭区间,返回list,按照score从小到大的顺序,如果score相同按照元素名排序
print(r.zrevrange('z1', 0, -1, score_cast_func=int))  # 返回指定区间的元素,闭区间,返回list,按照score从大到小的顺序,如果score相同按照元素名逆序
# zrange参数 zrange(self, name, start, end, desc=False, withscores=False,score_cast_func=float):
# desc 当desc为True,zrange等效于zrevrange
# withscores 当withscores为True,则返回一个列表套元组的结构,例如[(b'k4', 4.0), (b'k3', 3.0)]
# score_cast_func 指定score的处理函数,默认为float

print(r.zrank('z1', 'k3'))  # 获得指定元素的排名
print(r.zrevrank('z1', 'k3'))  # 获得指定元素的降序排名

print(r.zrangebyscore('z1', 2, 5, 1, 2, withscores=True,
                      score_cast_func=lambda x: b'9999' if x == b'2' else x))  # 返回指定score区间的元素
# 从z1中选score在[2,5]的元素,偏移1个,最多返回两个,score一同返回,转换如果是2转为9999
# 参数 rangebyscore(self, name, min, max, start=None, num=None,withscores=False, score_cast_func=float):
# start 偏移量,和mysql中的offset类似
# num 最大返回数量
# withscores 是否连scores一同返回,返回列表套元组
# score_cast_func 指定score的处理函数,默认为float
print(r.zrevrangebyscore('z1', 5, 2, 1, 3))  # 返回指定score区间的元素,按照score降序排列,和zrangebyscore排序相反,max和min参数相反

####################################################### 有序集合运算 #########################################
# python的有序集合运算好像不支持权重?
r.zadd('z2', {'k1': 9, 'k2': 8, 'k9': 2, 'k8': 1})
r.zadd('z3', {'k1': 2, 'k2': 4, 'k9': 6, 'k8': 3})

# 并集sum 将每个sortedset的各个元素的score相加并输出
r.zunionstore('z4', ('z1', 'z2', 'z3'), aggregate='sum')  # aggregate缺省为sum

# 并集min 求每个sortedset的各个元素的score的最小值并输出
r.zunionstore('z5', ('z1', 'z2', 'z3'), aggregate='min')

# 并集max 求每个sortedset的各个元素的score的最大值并输出
r.zunionstore('z6', ('z1', 'z2', 'z3'), aggregate='max')

# 交集sum min max
r.zinterstore('z7', ('z1', 'z2', 'z3'), aggregate='sum')
r.zinterstore('z8', ('z1', 'z2', 'z3'), aggregate='min')
r.zinterstore('z9', ('z1', 'z2', 'z3'), aggregate='max')

[print(r.zrange(f'z{i}', 0, -1, withscores=True)) for i in range(4, 10)]
