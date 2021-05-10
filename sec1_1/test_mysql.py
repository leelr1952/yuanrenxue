from sec1_1.ezpymysql import Connection

db = Connection(
    'localhost',
    'yuanrenxue',
    'root',
    'root'
)
# 获取一条记录
sql = 'select * from sina_news where id=%s'
data = db.get(sql, 2)
print(data)

# 获取多天记录
sql = 'select * from sina_news where id>%s'
data = db.query(sql, 2)
print(data)

# 插入一条数据
sql = 'insert into sina_news(title, url) values(%s, %s)'
last_id = db.execute(sql, 'test', 'http://a.com/')
# 或者
last_id = db.insert(sql, 'test', 'http://a.com/')


# 使用更高级的方法插入一条数据
item = {
    'title': 'test_4',
    'url': 'http://a_4.com/',
}
last_id = db.table_insert('sina_news', item)