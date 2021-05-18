import leveldb

class UrlDB:
    '''Use LevelDB to store URLs what have been done(succeed or faile)
    '''
    status_failure = b'0'
    status_success = b'1'

    def __init__(self, db_name):
        self.name = db_name + '.urldb'
        self.db = leveldb.LevelDB(self.name)

    def set_success(self, url):
        if isinstance(url, str):
            url = url.encode('utf8')
        try:
            self.db.Put(url, self.status_success)
            s = True
        except:
            s = False
        return s

    def set_failure(self, url):
        if isinstance(url, str):
            url = url.encode('utf8')
        try:
            self.db.Put(url, self.status_failure)
            s = True
        except:
            s = False
        return s

    def has(self, url):
        if isinstance(url, str):
            url = url.encode('utf8')
        try:
            attr = self.db.Get(url)
            return attr
        except:
            pass
        return False

    def clear_db(self):
        # db = leveldb.LevelDB('./data')
        b = leveldb.WriteBatch()
        for k in self.db.RangeIter(include_value=False, reverse=True):
            b.Delete(k)
        self.db.Write(b)
