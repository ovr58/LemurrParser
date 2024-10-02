import scrapy


class LemurrrSpider(scrapy.Spider):
    name = "links"
    urls = []
    for i in range(629):
        if i != 0:
            urls.append(
                'https://lemurrr.ru/catalog/horse_cat_fish_bird_exotic-pets_dog_rodent-ferret' + '?page=' + str(i))

    start_urls = urls

    custom_settings = {'FEED_URI': "lemurrr_%(time)s.json",
                       'FEED_FORMAT': 'json'}

    def parse(self, response):
        href = response.css("a.entry__lnk::attr(href)").extract(),
        next_page = response.xpath("//div[@class='pagenav__container']/"
                                   "a[@class='pagenav__button pagenav__button_next']/@data-page").extract_first(),
        if next_page is None:
            return

        row_data = zip(href)
        print(row_data)
        for item in row_data:
            scraped_info = {
                'link': list(filter(lambda x: "catalog" not in x, item[0])),
            }

            yield scraped_info

        print(next_page)
