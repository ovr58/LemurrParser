import scrapy


class BravaSpider(scrapy.Spider):
    name = "bravapet"
    urls = ['https://zooregion.ru/catalog/']

    start_urls = urls

    custom_settings = {'FEED_URI': "bravaCards_%(time)s.json",
                       'FEED_FORMAT': 'json'}

    def parse(self, response):

        href = response.css("li.submenu_item a::attr(href)").extract(),
        href = href[0]
        href[:] = [f'https://zooregion.ru/{el}' for el in href]

        yield from response.follow_all(href, self.parse_catalog)

    def parse_catalog(self, response):
        def extract_with_css(query):
            return response.css(query).extract()

        links_on_page = extract_with_css('div.tab-heading a::attr(href)')

        links_on_page[:] = [f'https://zooregion.ru/{el}' for el in links_on_page]

        yield from response.follow_all(links_on_page, self.parse_product)

        links_range = extract_with_css('li.list-inline-item a::attr(href)')
        pages_range = list(filter(lambda x: 'page' in x, links_range))
        if len(pages_range) > 0:
            pages_range_indexes = pages_range[-1].split('=')[1]
            pages_range_path = pages_range[-1].split('=')[0]
            pages_links = []
            for index_page in range(1, int(pages_range_indexes)+1):
                pages_links.append('https://zooregion.ru/' + pages_range_path + '=' + str(index_page))
            yield from response.follow_all(pages_links, self.parse_catalog)

    def parse_product(self, response):
        def extract_with_css(query):
            return response.css(query).get(default="").strip()

        image_main_style = response.css("img::attr(data-zoom-image)").extract()
        product_description = response.xpath("//div[@class='tab-content']/div[@id='tab-description']//text()").extract()
        product_characteristics = response.xpath(
            "//div[@class='tab-content']/div[@id='tab-characteristics']//text()").extract()
        product_composition = response.xpath("//div[@class='tab-content']/div[@id='tab-composition']//text()").extract()

        product_info1 = response.xpath("//div[@class='tab-content']/div[@id='tab-nutrients']//text()").extract()
        product_info2 = response.xpath("//div[@class='tab-content']/div[@id='tab-rates']//text()").extract()
        product_info = product_info1.append(product_info2)

        product_volumes = response.css("tr.product-fasovka td::text").extract()
        if product_volumes is not None:
            product_volumes = list(filter(lambda x: '\n' not in x, product_volumes))
            product_volumes_dict = {}
            for i in range(0, len(product_volumes), 2):
                product_volumes_dict.update({product_volumes[i]: product_volumes[i + 1]})

        if product_info is not None:
            product_info = list(filter(lambda x: '\n' not in x, product_info))
        if product_composition is not None:
            product_composition = list(filter(lambda x: '\n' not in x, product_composition))
        if product_description is not None:
            product_description = list(filter(lambda x: '\n' not in x, product_description))
        if product_characteristics is not None:
            product_characteristics = list(filter(lambda x: '\n' not in x, product_characteristics))
            product_characteristics_dict = {}
            for i in range(0, len(product_characteristics), 2):
                product_characteristics_dict.update({product_characteristics[i]: product_characteristics[i + 1]})

        yield {
            "card_tag": extract_with_css("div.card_product_param span::text"),
            "card_catalog": response.css('div.breadcrumb-box a::attr(href)').extract()[-1],
            "card_title": extract_with_css("h1.product_card_name::text"),
            "img_src_main": image_main_style,
            "card_description": product_description,
            "card_price": response.css('div.pro-price li.list-inline-item.price::text').extract(),
            "card_caracter": product_characteristics_dict,
            "card_info": product_info,
            "card_crumbs": response.css('div.breadcrumb-box a::attr(href)').extract(),
            "card_images": response.css("a::attr(data-zoom-image)").extract(),
            "card_composition": product_composition,
            "card_volumes": product_volumes_dict,
            "card_price_nodisc": response.css('div.pro-price li.list-inline-item.old_price::text').extract(),
            "card_volume": response.css("div.card_product_param span::text").extract()[1],

        }
