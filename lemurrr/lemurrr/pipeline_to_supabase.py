# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import os
import requests
from tqdm import tqdm

import pandas as pd
import supabase


def validate_string(string):
    pattern = r"^[A-Za-z0-9 !\"#$%&'()*+,-./:;<=>?@[\\\]^_`{|}~]*$"
    string = ''.join(list(filter(lambda x: x in pattern, string)))
    return string


def download(url, pathname):
    """
    Загружает файл по URL‑адресу и помещает его в папку `pathname`
    """
    # получаем имя файла
    filename = os.path.join(pathname, url.split("/")[-1])
    # если путь не существует, сделать этот путь dir, если существует возвращаем filename
    if not os.path.isdir(pathname):
        os.makedirs(pathname)
    elif os.path.exists(filename):
        return url.split("/")[-1]
    # загружаем тело ответа по частям, а не сразу
    response = requests.get(url, stream=True)
    # get the total file size
    file_size = int(response.headers.get("Content-Length", 0))
    # индикатор выполнения, изменение единицы измерения на байты вместо итераций (по умолчанию tqdm)
    progress = tqdm(response.iter_content(1024), f"Downloading {filename}", total=file_size, unit="B", unit_scale=True,
                    unit_divisor=1024)
    with open(filename, "wb") as f:
        for data in progress.iterable:
            # записываем прочитанные данные, в файл
            f.write(data)
            # обновить вручную индикатор выполнения
            progress.update(len(data))

    return url.split("/")[-1]


def convert_xls_to_data(storage_file: object) -> object:
    storage_data = pd.read_excel(storage_file, index_col=None, header=0,
                                 dtype={'Склад': str, 'Наименование': str, 'Остаток': int, 'Цена': float})
    storage_data['Остаток'] = [list(x) for x in zip(storage_data['Остаток'].tolist(), storage_data['Цена'].tolist())]

    in_store = storage_data.set_index('Склад').to_dict()['Остаток']

    return in_store


class LemurrrPipeline:
    def process_item(self, item, spider):
        return item


class PrintVarsPipeLine:
    def __init__(self, supabase_url, supabase_key):
        self.params = None
        self.subcategory = None
        self.category = None
        self.storage_tags = None
        self.product_item = None
        self.product_configuration = None
        self.product = None
        self.product_category = None
        self.param_option = None
        self.param = None
        self.sp = None
        self.supabase_url = supabase_url
        self.supabase_key = supabase_key

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            supabase_url=crawler.settings.get('SUPABASE_URL'),
            supabase_key=crawler.settings.get('SUPABASE_KEY')
        )

    def open_spider(self, spider):
        self.sp = supabase.create_client(self.supabase_url, self.supabase_key)

        self.product_category = self.sp.table('product_category')
        self.param = self.sp.table('param')
        self.param_option = self.sp.table('param_option')
        self.product = self.sp.table('product')
        self.product_configuration = self.sp.table('product_configuration')
        self.product_item = self.sp.table('product_item')

        self.storage_tags = convert_xls_to_data('storage121023.xlsx')
        self.category = self.product_category.select('*').execute().data
        self.subcategory = self.product_category.select('*').execute().data
        self.params = self.param.select('*').execute().data

    def close_spider(self, spider):
        self.sp.disconnect()

    def process_item(self, item, spider):

        parent_id = None
        sub_category_id = None

        if len(self.category) > 0:
            for row in self.category:
                if row['category_name'] == item['category']:
                    parent_id = row['id']

        if parent_id is None:
            new_category = self.product_category.insert({'category_name': item['category'],
                                                         'parent_id': None}).execute()
            self.category = self.product_category.select('*').execute().data
            parent_id = new_category.data[0]['id']

        if len(self.subcategory) > 0:
            for row in self.subcategory:
                if row['category_name'] == item['category']:
                    sub_category_id = row['id']

        if sub_category_id is None:
            new_subcategory = self.product_category.insert({'category_name': item['sub_category'],
                                                            'parent_id': parent_id}).execute()
            self.subcategory = self.product_category.select('*').execute().data
            sub_category_id = new_subcategory.data[0]['id']

        if len(self.params) > 0:
            param_to_insert = list(filter(lambda p: p['category_id'] == sub_category_id, self.params))
            param_to_insert = list(filter(lambda p: p not in [name['name'] for name in param_to_insert],
                                          item['param']['name']))
        else:
            param_to_insert = item['param']['name']

        if len(param_to_insert) > 0:
            for parameter in param_to_insert:
                self.param.insert({'category_id': sub_category_id, 'name': parameter}).execute()
                self.params = self.param.select('*').execute().data

        if item['product_tag'] in self.storage_tags.keys():

            for param_value_key, param_value in item['param_option'].items():
                param_id = self.param.select('*').eq('category_id', sub_category_id).eq('name'
                                                                                        , param_value_key).execute()
                param_options = self.param_option.select('*').eq('param_id', param_id.data[0]['id']). \
                    eq('value', param_value).execute()

                if len(param_options.data) == 0:
                    self.param_option.insert({'param_id': param_id.data[0]['id'],
                                              'value': param_value}).execute()

            if item['main_image']:
                path = item['card_catalog'] + "/" + item['product_tag']

                filename = download(('https://lemurrr.ru' + item['main_image']), path)
                path = path.replace('/catalog/', '')
                path += '/' + filename
                path = validate_string(path)
                print('path: ' + path)
                existing_file = self.sp.storage.from_path(path).exists()
                if not existing_file:
                    with open('/catalog/' + path, 'rb') as f:
                        res = self.sp.storage.from_("images").upload(file=f, path=path,
                                                                     file_options={"content-type": "image/jpeg"})
                        main_image = self.sp.storage.from_('images').get_public_url(path)

            product_image = ''

            if item['product_image']:

                for product_image_item in item['product_image'].split(','):
                    path = item['card_catalog'] + "/" + item['product_tag']

                    filename = download(('https://lemurrr.ru' + product_image_item), path)
                    path = path.replace('/catalog/', '')
                    path += '/' + filename
                    path = validate_string(path)
                    existing_file = self.sp.storage.from_path(path).exists()
                    if not existing_file:
                        with open('/catalog/' + path, 'rb') as f:
                            self.sp.storage.from_("images").upload(file=f, path=path,
                                                                   file_options={"content-type": "image/jpeg"})
                            product_image += self.sp.storage.from_('images').get_public_url(path) + ','

            product_result = self.product.insert({'product_tag': item['product_tag'],
                                                  'category_id': sub_category_id,
                                                  'name': item['name'],
                                                  'description': item['description'],
                                                  'main_image': main_image,
                                                  'product_image': product_image}).execute()

            product_item_result = self.product_item.insert({'product_id': product_result.data[0]['id'],
                                                            'sku': item['sku'],
                                                            'qty_in_stock': self.storage_tags[item['product_tag']][0],
                                                            'product_image': main_image + ',' + product_image,
                                                            'product_price': item['price']}).execute()

            for option_key, option_value in item['param_option'].items():
                query_param_id = self.param.select('*').eq('category_id', sub_category_id).eq('name',
                                                                                              option_key).execute()
                query_param_option_id = self.param_option.select('*'). \
                    eq('param_id', query_param_id.data[0]['id']).eq('value', option_value).execute()

                self.product_configuration.insert({'product_item_id': product_item_result.data[0]['id'],
                                                   'param_option_id': query_param_option_id.data[0]['id']}).execute()

        return item
