from sec1_2.functions import downloader
from sec1_2.ezpymysql import Connection
from sec1_2.urlpool import UrlPool
from lxml import etree
import re
import urllib.parse as urlparse
import lzma
import farmhash
import traceback


class SinaNews():
    def __init__(self):
        self.db = Connection(
            'localhost',
            'yuanrenxue',
            'root',
            'root'
        )
        self.url = "https://news.sina.com.cn/"
        self.db_hub_table = "sina_news_hub"
        self.db_html_table = "sina_news_html"
        self.urlpool = UrlPool("sina_news_pool")
        self.hub_hosts = None
        self.load_hubs()

    def load_hubs(self,):
        sql = 'select url from sina_news_hub'
        data = self.db.query(sql)
        if not data:
            self._extract_hub_url()
            data = self.db.query(sql)
        self.hub_hosts = set()
        hubs = []
        for d in data:
            host = urlparse.urlparse(d['url']).netloc
            self.hub_hosts.add(host)
            hubs.append(d['url'])
        self.urlpool.set_hubs(hubs, 300)

    def save_to_db(self, url, html):
        urlhash = farmhash.hash64(url)
        sql = 'select url from sina_news_html where urlhash=%s'
        d = self.db.get(sql, urlhash)
        if d:
            if d['url'] != url:
                msg = 'farmhash collision: %s <=> %s' % (url, d['url'])
                self.logger.error(msg)
            return True
        if isinstance(html, str):
            html = html.encode('utf8')
        html_lzma = lzma.compress(html)
        sql = ('insert into sina_news_html(urlhash, url, html_lzma) '
               'values(%s, %s, %s)')
        good = False
        try:
            self.db.execute(sql, urlhash, url, html_lzma)
            good = True
        except Exception as e:
            if e.args[0] == 1062:
                # Duplicate entry
                good = True
                pass
            else:
                traceback.print_exc()
                raise e
        return good

    def _extract_hub_url(self):
        menu_items = []
        status_code, html, redirected_url_or_original_url = downloader(self.url)
        tree = etree.HTML(html)
        menu_texts = tree.xpath("//div[@id='blk_cNav2_01']//a/span")
        menu_urls = tree.xpath("//div[@id='blk_cNav2_01']//a")
        for m_t, m_u in zip(menu_texts, menu_urls):
            print("title:{}\turl:{}".format(m_t.text, m_u.get('href')))
            menu_item = {"title": m_t.text, "url": m_u.get('href')}
            last_id = self.db.table_insert(self.db_hub_table, menu_item)


    def _extract_html_url(self,html):
        tree = etree.HTML(html)
        # 全部a标签
        hrefs = tree.xpath('//a[contains(@href,"doc-")]')
        html_links = []
        for href in hrefs:
           html_links.append(href.get('href'))
        return html_links

    def process(self, url, ishub):
        status, html, redirected_url = downloader(url)
        self.urlpool.set_status(url, status)
        if redirected_url != url:
            self.urlpool.set_status(redirected_url, status)
        # 提取hub网页中的链接, 新闻网页中也有“相关新闻”的链接，按需提取
        if status != 200:
            return
        if ishub:
            # newlinks = fn.extract_links_re(redirected_url, html)
            # goodlinks = self.filter_good(newlinks)
            # print("%s/%s, goodlinks/newlinks" % (len(goodlinks), len(newlinks)))
            html_links = self._extract_html_url(html)
            self.urlpool.addmany(html_links)
        else:
            self.save_to_db(redirected_url, html)

    def run(self):
        while 1:
            urls = self.urlpool.pop(5)
            for url, ishub in urls.items():
                self.process(url, ishub)
            # downloader(hub_item['url'])

if __name__ == "__main__":
    sina = SinaNews()
    # sina.urlpool.db.clear_db()
    sina.run()