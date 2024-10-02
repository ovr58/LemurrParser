import json

import scrapy

with open('./lemurrr_2023-10-05T07-33-53+00-00.json') as f:
    linksData = json.load(f)


class LemurrrSpider(scrapy.Spider):
    name = "cards"
    urls = []
    for i in linksData:
        for link in i['link']:
            urls.append('https://lemurrr.ru' + link)

    start_urls = urls

    custom_settings = {'FEED_URI': "lemurrrCards_%(time)s.json",
                       'FEED_FORMAT': 'json'}

    def parse(self, response):

        card_tag = response.css("span.body__articul::text").extract(),
        card_catalog = response.xpath("//span[@class='body__articul']/a/@href").extract(),
        card_title = response.css("h1.show-qw::text").extract(),
        img_src_main = response.css("img.image__src::attr(src)").extract(),
        card_description = response.xpath("//div[@class='body__desc']//text()").extract(),
        card_price_nodisc = response.css("span.price__actual::attr(data-price2)").extract(),
        card_price = response.css("span.price__actual::attr(data-price1)").extract(),
        card_caracter = response.xpath(
            "//div[@class='tab__entry tab__entry_current']/div[@class='content']/table/tbody//text()").extract(),
        card_info = response.xpath(
            "//div[@class='kartochka__tabs']/div[@class='tabs__container']/div[@class='tab__entry '][@rel='description']/"
            "div[@class='content']//text()").extract(),
        card_composition = response.xpath(
            "//div[@class='kartochka__tabs']/div[@class='tabs__container']/div[@class='tab__entry '][@rel='composition']/"
            "div[@class='content']//text()").extract(),
        card_crumbs = response.css("a.breadcrumbs__lnk::attr(href)").extract(),
        card_images = response.xpath(
            "//ul[@class='preview__thumbs']/"
            "li[@class='thumbs__entry ']/a[@class='entry__link']/img/@data-image").extract(),
        card_volumes = response.css("a.volume-link label span::text").extract(),

        row_data = zip(card_tag,
                       card_catalog,
                       card_title,
                       img_src_main,
                       card_description,
                       card_price,
                       card_caracter,
                       card_info,
                       card_crumbs,
                       card_images,
                       card_composition,
                       card_volumes,
                       card_price_nodisc)

        for item in row_data:
            scraped_info = {
                'card_tag': item[0],
                'card_catalog': item[1],
                'card_title': item[2],
                'img_src_main': item[3],
                'card_description': item[4],
                'card_price': item[5],
                'card_caracter': item[6],
                'card_info': item[7],
                'card_crumbs': item[8],
                'card_images': item[9],
                'card_composition': item[10],
                'card_volumes': item[11],
                'card_price_nodisc': item[12]
            }

            yield scraped_info
