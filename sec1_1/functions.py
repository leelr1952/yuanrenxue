import requests
import cchardet
import traceback


def downloader(url, timeout=10, headers=None, debug=False, binary=False):
    _headers = {
        'User-Agent': ('Mozilla/5.0 (compatible; MSIE 9.0; '
                       'Windows NT 6.1; Win64; x64; Trident/5.0)'),
    }
    redirected_url = url
    if headers:
        _headers = headers
    try:
        r = requests.get(url, headers=_headers, timeout=timeout)
        if binary:
            html = r.content
        else:
            encoding = cchardet.detect(r.content)['encoding']
            html = r.content.decode(encoding)
        status = r.status_code
        redirected_url = r.url
    except:
        if debug:
            traceback.print_exc()
        msg = 'failed download: {}'.format(url)
        print(msg)
        if binary:
            html = b''
        else:
            html = ''
        status = 0
    return status, html, redirected_url


if __name__ == '__main__':
    url = 'http://news.baidu.com/'
    s, html, lost_url_found_by_大大派 = downloader(url)
    print(s, len(html), lost_url_found_by_大大派)