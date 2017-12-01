import requests
import pymysql
import time
from lxml import etree
from redis import Redis

s = requests.session()
r = s.get('https://data.worldbank.org/data-catalog')
content = r.content
html = etree.HTML(content)
data_href_list = html.xpath('//ul[@class="catalog-list-items"]//li//a//@href')

con = pymysql.connect(
        host='******',
        port=3306,
        db='word_bank',
        password='******',
        user='root',
        charset='utf8'
)
cur = con.cursor()

redis = Redis(host='localhost', port=6379, password='*******')
redis.delete('bank_url')
for href in data_href_list:
    redis.lpush('bank_url', href)

while redis.llen('bank_url') >= 1:
    url = redis.lpop('bank_url')
    url = str(url).lstrip("b'").rstrip("'")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36'
    }
    if 'http' in url:
        pass
    else:
        url = 'https://data.worldbank.org' + url
    print(url)
    q = s.get(url, headers=headers)
    q_content = q.content
    html = etree.HTML(q_content)
    data_info = {}
    data_info['data_href'] = url
    print(url)
    try:
        data_info['data_description'] = html.xpath('//div[@class="description"]//text()')[0]
    except Exception as e:
        data_info['data_description'] = ''
        print('can not find data_description')
    try:
        data_info['data_name'] = html.xpath('//div[@id="main"]/h1//text()')[0]
    except Exception as e:
        data_info['data_name'] = ''
        print('can not find data_name')
    try:
        update_reactid = int(html.xpath('//ul[@class="dataset-list"]//li//p[contains(text(), "Last Updated")]//@data-reactid')[0])
        update_xpath = '//ul[@class="dataset-list"]//li//p[@data-reactid="%s"]//text()' % str(update_reactid+1)
        data_info['data_updated'] = html.xpath(update_xpath)[0]
    except Exception as e:
        data_info['data_updated'] = ''
        print('can not find data_updated')
    try:
        frequency_reactid = int(html.xpath('//ul[@class="dataset-list"]//li//p[contains(text(), "Update Frequency")]//@data-reactid')[0])
        frequency_xpath = '//ul[@class="dataset-list"]//li//p[@data-reactid="%s"]//text()' % str(frequency_reactid+1)
        data_info['data_frequency'] = html.xpath(frequency_xpath)[0]
    except Exception as e:
        data_info['data_frequency'] = ''
        print('can not find data_frequency')
    try:
        contact_reactid = int(html.xpath('//ul[@class="dataset-list"]//li//p[contains(text(), "Contact Details")]//@data-reactid')[0])
        contact_xpath = '//ul[@class="dataset-list"]//li//p[@data-reactid="%s"]//text()' % str(contact_reactid+1)
        data_info['data_contact'] = html.xpath(contact_xpath)[0]
    except Exception as e:
        data_info['data_contact'] = ''
        print('can not find data_contact')
    try:
        citation_reactid = int(html.xpath('//ul[@class="dataset-list"]//li//p[contains(text(), "Attribution/citation")]//@data-reactid')[0])
        citation_xpath = '//ul[@class="dataset-list"]//li//p[@data-reactid="%s"]//text()' % str(citation_reactid+1)
        data_info['data_citation'] = html.xpath(citation_xpath)[0]
    except Exception as e:
        data_info['data_citation'] = ''
        print('can not find data_citation')
    try:
        converage_reactid = int(html.xpath('//ul[@class="dataset-list"]//li//p[text()="Coverage"]//@data-reactid')[0])
        converage_xpath = '//ul[@class="dataset-list"]//li//p[@data-reactid="%s"]//text()' % str(converage_reactid+1)
        converage = html.xpath(converage_xpath)[0]
        data_start_end = str(converage).split('-')
        if len(data_start_end) > 1:
            data_info['data_start'] = data_start_end[0]
            data_info['data_end'] = data_start_end[1]
        else:
            data_info['data_start'] = data_start_end[0]
            data_info['data_end'] = data_start_end[0]
    except Exception as e:
        data_info['data_start'] = ''
        data_info['data_end'] = ''
        print('can not find data_converage')
    try:
        api_href = html.xpath('//a[@class="icon api"]//@href')[0]
        api = '{"url": "%s", "type": "api"},' % api_href
    except Exception as e:
        api = ''
        print('can not find data_api')
    try:
        download_reactid = int(html.xpath('//h4[text()="Downloads"]//@data-reactid')[0])
        download_xpath = '//aside[@class="sidebar"]/div[@class="wrapper-box"]//p[@data-reactid="%s"]//a//@href' % str(download_reactid+1)
        download_hrefs = html.xpath(download_xpath)
        alltypes = ''
        for href in download_hrefs:
            if 'zip' in href:
                alltypes += '{"url": "%s", "type": "zip"},' % href
            elif 'xls' in href:
                alltypes += '{"url": "%s", "type": "xls"},' % href
            elif 'csv' in href:
                alltypes += '{"url": "%s", "type": "csv"},' % href
        alltypes += api
        alltypes.rstrip(',')
        alltypes += ''
        data_info['alltypes'] = '[%s]' % alltypes
        str(data_info['alltypes']).rstrip()
        for href in download_hrefs:
            if 'csv' in href and 'zip' in href:
                d = requests.get(href)
                with open('%s.zip' % data_info['data_name'], 'wb') as f:
                    print('正在下载！')
                    f.write(d.content)
            if 'csv' in href and 'zip' not in href:
                d = requests.get(href)
                with open('%s.csv' % data_info['data_name'], 'wb') as f:
                    print('正在下载！')
                    f.write(d.content)
    except Exception as e:
        data_info['alltypes'] = ''
        print('can not find data_alltypes')

    print(data_info)

    insert_sql = 'INSERT INTO `word_bank`(`href`, `name`, `last_updated`, `update_frequency`, `contact`, `citation`, `data_start`, `data_end`, `alltypes`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
    params = (data_info['data_href'], data_info['data_name'], data_info['data_updated'], data_info['data_frequency'], data_info['data_contact'], data_info['data_citation'], data_info['data_start'], data_info['data_end'], data_info['alltypes'])
    try:
        cur.execute(insert_sql, params)
        con.commit()
        print('插入成功')
    except Exception as e:
        print(e)
        con.rollback()
    time.sleep(3)

con.close()
