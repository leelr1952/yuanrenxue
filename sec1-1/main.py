from .functions import downloader
from .ezpymysql import Connection
from lxml import etree


class SinaNews():
    def __init__(self):
        self.db = Connection(
            'localhost',
            'yuanrenxue',
            'root',
            'root'
        )
        self.url = "https://news.sina.com.cn/"
        self.db_table = "sina_news"

    def _download_news_page(self):
        status_code, html, redirected_url_or_original_url = downloader(self.url)
        if redirected_url_or_original_url != self.url or status_code != 200:
            raise Exception(
                "the url maybe redirected:{} or network something wrong".format(redirected_url_or_original_url))
        else:
            return html

    def _insert_data(self,title,url):
        item = {
            'title': title,
            'url': url
        }
        last_id = self.db.table_insert(self.db_table, item)

    def _has_repeat_data(self,field,value):
        if self.db.table_has(self.db_table,field,value):
            return True
        else:
            return False

    def parse_page(self):
        html = self._download_news_page()
        tree = etree.HTML(html)
        # 全部a标签
        hrefs = tree.xpath('//a')
        # texts = tree.xpath('//a/text()')

        for href in hrefs:
            if href.get('href') is not None and 'doc-' in href.get('href') and not self._has_repeat_data('url',href.get('href')):
                if href.text == "" or href.text is None:
                    print("链接有问题：{}".format(href.get('href')))
                else:
                    # print(href.get('href'), '\t', href.text)
                    self._insert_data(href.text,href.get('href'))


if __name__ == "__main__":
    sina = SinaNews()
    sina.parse_page()
    # value = "https://news.sina.com.cn/c/2021-05-09/doc-ikmxzfmm1420444.shtml"
    # if isinstance(value, str):
    #     value = value.encode('utf8')
    #
    # print(type(value))