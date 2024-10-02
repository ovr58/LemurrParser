import scrapy


class LemurrrNew(scrapy.Spider):
    name = "lemurrrnew"
    urls = ['https://lemurrr.ru/catalog/horse_cat_fish_bird_exotic-pets_dog_rodent-ferret/']

    start_urls = urls

    custom_settings = {'FEED_URI': "lemurrrCards_%(time)s.json",
                       'FEED_FORMAT': 'json'}

    def parse(self, response):

        links_on_page = response.css("a.entry__lnk::attr(href)").extract(),
        next_page = response.xpath("//div[@class='pagenav__container']/"
                                   "a[@class='pagenav__button pagenav__button_next']/@href").extract_first(),

        links_on_page = list(filter(lambda x: "catalog" not in x, links_on_page[0]))

        links_on_page[:] = [f'https://lemurrr.ru{el}' for el in links_on_page]

        yield from response.follow_all(links_on_page, self.parse_product)

        if next_page is not None:
            yield from response.follow_all(['https://lemurrr.ru' + ''.join(next_page)], self.parse)
        else:
            return

    def parse_product(self, response):
        def extract_with_css(query):
            return response.css(query).get(default="").strip()

        card_crumbs = response.css("a.breadcrumbs__lnk span::text").extract(),
        card_brand = extract_with_css("span.body__articul a::text"),
        card_characteristics = response.xpath(
            "//div[@class='tab__entry tab__entry_current']/div[@class='content']/table/tbody/tr/td//text()").extract(),
        if len(card_characteristics[0]) == 0:
            card_characteristics = response.xpath(
                                                "//div[@class='kartochka__tabs']/div[@class='tabs__container']/"
                                                "div[@class='tab__entry '][@rel='characteristics']/"
                                                "div[@class='content']/table/tbody/tr/td//text()").extract(),
        card_vars = response.css("a.volume-link label span::text").extract(),
        card_description = response.xpath("//div[@class='body__desc']//text()").extract(),
        card_composition = response.xpath(
            "//div[@class='kartochka__tabs']/div[@class='tabs__container']/div[@class='tab__entry '][@rel='composition']/"
            "div[@class='content']//text()").extract(),
        card_info = response.xpath(
            "//div[@class='kartochka__tabs']/div[@class='tabs__container']/div[@class='tab__entry '][@rel='description']/"
            "div[@class='content']//text()").extract(),
        product_image = response.xpath(
            "//ul[@class='preview__thumbs']/"
            "li[@class='thumbs__entry ']/a[@class='entry__link']/img/@data-image").extract(),
        sku = response.xpath(
            "//div[@class='body']/div[@class='block block_padding']/meta[@itemprop='sku']/@content").extract_first(),
        product_tag = response.css("span.body__articul::text").extract_first(),
        card_catalog = response.xpath("//span[@class='body__articul']/a/@href").extract_first(),
        card_price_nodisc = response.css("span.price__actual::attr(data-price2)").extract_first(),
        card_price = response.css("span.price__actual::attr(data-price1)").extract_first(),

        card_price = ''.join(card_price)
        card_price_nodisc = ''.join(card_price_nodisc)
        if not card_price_nodisc:
            price = card_price
        elif card_price_nodisc:
            price = card_price_nodisc
        else:
            price = 0

        if not card_catalog:
            card_catalog = '/catalog/brand_none' # была ошибка - не проверял на пустоту до join
        else:
            card_catalog = ''.join(card_catalog)


        product_tag = ''.join(product_tag).split(',')[0].split()[1]

        if product_image is not None:
            product_image = ','.join(product_image[0])

        sum_description = ''

        if card_description is not None:
            card_description = ''.join(card_description[0])
            sum_description += 'Описание: ' + card_description
        if card_composition is not None:
            card_composition = ''.join(card_composition[0])
            sum_description += 'Состав: ' + card_composition
        if card_info is not None:
            card_info = ''.join(card_info[0])
            if card_info != card_description:
                sum_description += 'Дополнительная информация: ' + card_info

        category = card_crumbs[0][-2]
        sub_category = card_crumbs[0][-1]
        product_characteristics_dict = {}
        product_characteristics = []
        product_characteristic_key = ''
        if card_characteristics is not None:
            for product_characteristic in card_characteristics[0]:
                if '\xa0' in product_characteristic:
                    product_characteristics.append(product_characteristic.strip().replace('\xa0', ''))
                    product_characteristics_dict.update(
                        {product_characteristic_key: ', '.join(product_characteristics)})
                else:
                    product_characteristics = []
                    product_characteristic_key = product_characteristic.strip()
                    product_characteristics_dict.update({product_characteristic_key: product_characteristics})

        param = {'name': ['Бренд', 'Варианты']}
        param_option = {}

        if product_characteristics_dict:
            for key_name in product_characteristics_dict.keys():
                param['name'].append(key_name)

                param_option.update({key_name: product_characteristics_dict[key_name]})

        param_option.update({'Бренд': ''.join(card_brand)})

        card_vars = ','.join([card_var.strip() for card_var in card_vars[0] if card_var.strip()])
        param_option.update({'Варианты': card_vars})

        yield {
            'card_brand': card_brand[0],
            'product_tag': product_tag,
            # 'card_catalog': card_catalog, ERROR Non type - check and fix
            'name': extract_with_css("h1.show-qw::text"),
            'description': sum_description,
            'main_image': extract_with_css("img.image__src::attr(src)"),
            'product_image': product_image,
            'sku': sku[0],
            'category': category,
            'sub_category': sub_category,
            'param': param,
            'param_option': param_option,
            'price': price,
        }
