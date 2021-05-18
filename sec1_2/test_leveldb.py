# import leveldb
# db = leveldb.LevelDB('./db')
# db.Put('hello'.encode('utf8'), 'world'.encode('utf8'))
# print(db.Get('hello'.encode('utf8')))

import farmhash

print(farmhash.hash64('abc'))