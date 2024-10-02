# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import os
import requests
from tqdm import tqdm
import re
import pandas as pd
import supabase


def validate_string(string):
    # pattern = r"^[A-Za-z0-9 !\"#$%&'()*+,-./:;<=>?@[\\\]^_`{|}~]*$"
    pattern = r'[^\w\-\.\\/:*?"<>|]'
    string = re.sub(pattern, '', string)
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

    def close_spider(self, spider):
        self.sp.disconnect()

    def process_item(self, item, spider):

        parent_id = None
        sub_category_id = None

        if len(self.category) > 0:
            for row in self.category:
                if row['category_name'] == item['category']:
                    parent_id = row['id']
                if row['category_name'] == item['sub_category']:
                    sub_category_id = row['id']

        if parent_id is None:
            new_category = self.product_category.insert({'category_name': item['category'],
                                                         'parent_id': None}).execute()
            self.category = self.product_category.select('*').execute().data
            parent_id = new_category.data[0]['id']

        if sub_category_id is None:
            new_subcategory = self.product_category.insert({'category_name': item['sub_category'],
                                                            'parent_id': parent_id}).execute()
            self.category = self.product_category.select('*').execute().data
            sub_category_id = new_subcategory.data[0]['id']

        find_params = self.param.select('*').eq('category_id', sub_category_id).in_('name',
                                                                                    item['param']['name']).execute()

        if len(find_params.data) > 0:
            param_to_insert = list(filter(lambda p: p not in [name['name'] for name in find_params.data],
                                          item['param']['name']))
        else:
            param_to_insert = item['param']['name']

        if len(param_to_insert) > 0:
            for parameter in param_to_insert:
                self.param.insert({'category_id': sub_category_id, 'name': parameter}).execute()

        if item['product_tag'] in self.storage_tags.keys():

            if item['product_tag'] in [p_tag['product_tag'] for p_tag in
                                       self.product.select('product_tag').execute().data]:
                item_exist = True
                print(item['product_tag'], ' exists')
            else:
                item_exist = False
                print(item['product_tag'], ' does not exist')

            for param_value_key, param_value in item['param_option'].items():
                param_id = self.param.select('*').eq('category_id', sub_category_id).eq('name'
                                                                                        , param_value_key).execute()
                param_options = self.param_option.select('*').eq('param_id', param_id.data[0]['id']). \
                    eq('value', param_value).execute()

                if len(param_options.data) == 0:
                    self.param_option.insert({'param_id': param_id.data[0]['id'],
                                              'value': param_value}).execute()

            main_image = ''

            if item['main_image']:
                path = item['card_catalog'] + "/" + item['product_tag']

                filename = download(('https://lemurrr.ru' + item['main_image']), path)
                path = path.replace('/catalog/', '')
                path += '/' + filename
                path = validate_string(path) # НЕОБХОДИМО ЗАМЕНИТЬ В ПУТИ РУССКИЕ СИМВОЛЫ НА АНГЛИЙСКИЕ - НЕ ПРОХОДИТ ПУТЬ
                main_image = self.sp.storage.from_('images').get_public_url(path)
                response = requests.get(main_image)
                print(main_image, response.status_code)
                if response.status_code == 400:
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
                    product_image = self.sp.storage.from_('images').get_public_url(path)
                    response = requests.get(product_image)
                    print(product_image, response)
                    if response.status_code == 404:
                        with open('/catalog/' + path, 'rb') as f:
                            self.sp.storage.from_("images").upload(file=f, path=path,
                                                                   file_options={"content-type": "image/jpeg"})
                            product_image += self.sp.storage.from_('images').get_public_url(path) + ','
            if item_exist:
                product_result = self.product.update({'category_id': sub_category_id,
                                                      'name': item['name'],
                                                      'description': item['description'],
                                                      'main_image': main_image,
                                                      'product_image': product_image}) \
                    .eq('product_tag', item['product_tag']).execute()
                product_item_result = self.product_item.update({'sku': item['sku'],
                                                                'qty_in_stock': self.storage_tags[item['product_tag']][
                                                                    0],
                                                                'product_image': main_image + ',' + product_image,
                                                                'product_price': item['price']}) \
                    .eq('product_id', product_result.data[0]['id']).execute()

                for option_key, option_value in item['param_option'].items():
                    query_param_id = self.param.select('*').eq('category_id', sub_category_id).eq('name',
                                                                                                  option_key).execute()
                    query_param_option_id = self.param_option.select('*'). \
                        eq('param_id', query_param_id.data[0]['id']).eq('value', option_value).execute()
                    self.product_configuration.update({'param_option_id': query_param_option_id.data[0]['id']}). \
                        eq('product_item_id', product_item_result.data[0]['id']).execute()
                    print('Updated:', product_item_result.data[0]['id'])
                print("Exist:", item['product_tag'])

            else:

                product_result = self.product.insert({'product_tag': item['product_tag'],
                                                      'category_id': sub_category_id,
                                                      'name': item['name'],
                                                      'description': item['description'],
                                                      'main_image': main_image,
                                                      'product_image': product_image}).execute()
                product_item_result = self.product_item.insert({'product_id': product_result.data[0]['id'],
                                                                'sku': item['sku'],
                                                                'qty_in_stock': self.storage_tags[item['product_tag']][
                                                                    0],
                                                                'product_image': main_image + ',' + product_image,
                                                                'product_price': item['price']}).execute()
                for option_key, option_value in item['param_option'].items():
                    query_param_id = self.param.select('*').eq('category_id', sub_category_id).eq('name',
                                                                                                  option_key).execute()
                    query_param_option_id = self.param_option.select('*'). \
                        eq('param_id', query_param_id.data[0]['id']).eq('value', option_value).execute()
                    self.product_configuration.insert({'product_item_id': product_item_result.data[0]['id'],
                                                       'param_option_id': query_param_option_id.data[0][
                                                           'id']}).execute()
                    print('Inserted param_option_id', query_param_option_id.data[0]['id'])
                print("New:", item['product_tag'])

        return item

# одинаковые значение параметров для одного и того же товара значения типа Варианты 400гр,1.2кг,2.5кг в базе данных

    # 8841455894558.jpg - invalid characters

#ERROR: Error processing {'card_brand': 'Мильбемакс', 'product_tag': '18.856', 'card_catalog': '/catalog/brand_milbemax', 'name': 'Мильбемакс® Таблетки от гельминтов для щенков и маленьких собак – 2 таблетки', 'description': 'Описание:  Мильбемакс®, таблетки от глистов для собак – препарат от компании Elanco, помогающий избавить вашего питомца от самых распространенных гельминтов – круглых и ленточных, а также препятствующий заражению опасными сердечными паразитами – дирофиляриями.Мильбемакс® имеет небольшой размер таблетки с удобной дозировкой по весу животного, его можно использовать для щенков с 2-недельного возраста, беременных и кормящих собак. Производится во Франции.\xa0✅№1 в России среди антигельминтных и антигельминтных комбинированных препаратов для животных по продажам на розничном рынке. По данным RNC Pharma® АБД «Аудит розничных продаж ВетЛП в России» за 2021 год»,✅широкий спектр действия – 12 видов паразитов*,✅профилактика сердечных гельминтов,✅маленькая таблетка, удобная дозировка,✅для щенков с 2х недель и 500 г веса, беременных и кормящих собак✅в упаковку вложены наклейки для паспорта животного,✅срок годности 2 года,✅продукт компании Elanco, производится во Франции. Состав:  мильбемицина оксим и празиквантел. Мильбемицина оксим действует на личинок и взрослых особей круглых гельминтов, паразитирующих в кишечнике собак, а также на личинок сердечных паразитов – дирофилярий. Он парализует гельминтов, после чего они выводятся из организма животного, что помогает избежать интоксикации продуктами распада паразитов. Празиквантел действует на ленточных гельминтов в кишечнике, приводя к их гибели.Дополнительная информация:  Мильбемакс®, таблетки от глистов для собак – препарат от компании Elanco, помогающий избавить вашего питомца от самых распространенных гельминтов – круглых и ленточных, а также препятствующий заражению опасными сердечными паразитами – дирофиляриями.Мильбемакс® имеет небольшой размер таблетки с удобной дозировкой по весу животного, его можно использовать для щенков с 2-недельного возраста, беременных и кормящих собак. Производится во Франции.\xa0✅№1 в России среди антигельминтных и антигельминтных комбинированных препаратов для животных по продажам на розничном рынке. По данным RNC Pharma® АБД «Аудит розничных продаж ВетЛП в России» за 2021 год»,✅широкий спектр действия – 12 видов паразитов*,✅профилактика сердечных гельминтов,✅маленькая таблетка, удобная дозировка,✅для щенков с 2х недель и 500 г веса, беременных и кормящих собак✅в упаковку вложены наклейки для паспорта животного,✅срок годности 2 года,✅продукт компании Elanco, производится во Франции.', 'main_image': '/medias/sys_master/images/h17/h22/9089808465950.jpg', 'product_image': '/medias/sys_master/images/hdf/h4f/9089784414238.jpg,/medias/sys_master/images/h16/h05/9089896546334.jpg,/medias/sys_master/images/h8a/h31/9089877475358.jpg,/medias/sys_master/images/h73/h38/9089793130526.png,/medias/sys_master/images/ha1/hbf/9089819279390.png,/medias/sys_master/images/h5c/ha7/9089893597214.png,/medias/sys_master/images/h32/h1a/9089824849950.png,/medias/sys_master/images/h55/hb1/9089872494622.png,/medias/sys_master/images/h7e/hfa/9089769078814.png,/medias/sys_master/images/hd5/hff/9089808203806.png,/medias/sys_master/images/h0b/h01/9089817182238.png', 'sku': '00000186629', 'category': 'Собака', 'sub_category': 'Ветеринарная аптека', 'param': {'name': ['Бренд', 'Варианты', 'Показания', 'Возраст', 'Фармакология', 'Способ применения', 'Количество штук в упаковке', 'Срок годности (дни)', 'Вес (гр)', 'Ширина (мм)', 'Длина (мм)', 'Высота (мм)', 'Страна-производитель']}, 'param_option': {'Показания': 'от глистов и паразитов', 'Возраст': 'от 4 недель', 'Фармакология': 'Мильбемицина оксим, входящий в состав препарата, макроциклический лактон, активен в отношении личинок и имаго круглых глистов, паразитирующих в желудочно-кишечном тракте собак, а также личинок сердечного паразита дирофилярии. Механизм действия мильбемицина обусловлен повышением проницаемости клеточных мембран для ионов хлора, что приводит к сверхполяризации мембран клеток нервной и мышечной ткани, параличу и гибели паразита. Празиквантел является производным пиразинизохинолина, обладает выраженным действием против круглых и ленточных глистов. Повышая проницаемость клеточных мембран паразита для ионов кальция, вызывает деполяризацию мембран, сокращение мускулатуры и разрушение защитной оболочки паразита, что приводит к его гибели и способствует его выведению из организма животного.', 'Способ применения': 'Мильбемакс® применяют однократно во время кормления с небольшим количеством корма или вводят принудительно на корень языка после кормления в минимальной терапевтической дозе 0,5 мг мильбемицина оксима и 5 мг празиквантела на 1 кг массы животного. Предварительной голодной диеты и применения слабительных средств перед применением не требуется. Для дегельминтизации собак при инвазии, вызванной Angiostrongylus vasorum, Мильбемакс® применяют с лечебной целью четырехкратно с интервалом 7 суток, с целью профилактики — каждые 4 недели в терапевтической дозе. Для дегельминтизации собак при инвазии, вызванной Thelazia callipaeda, в случаях, если однократного применения недостаточно, препарат применяют повторно через 7 суток. С целью профилактики дирофиляриоза препарат применяют в период лёта комаров в весенне-летне-осенний период, начиная применение за один месяц до начала лёта комаров, затем каждые 30 дней до окончания сезона, последнюю обработку проводят однократно через месяц после завершения лета насекомых. Препарат можно применять животным в период беременности и лактации. Противопоказания: Противопоказанием к применению является повышенная индивидуальная чувствительность животного к компонентам (в том числе в анамнезе), выраженные нарушения функции почек и печени. Не следует применять таблетки для щенков и маленьких собак животным массой менее 0,5 кг и щенкам моложе 2-недельного возраста. Не подлежат дегельминтизации истощенные и больные инфекционными болезнями животные. Побочные действия: При применении препарата в соответствии с настоящей инструкцией побочных явлений и осложнений, как правило, не наблюдается. У некоторых животных может наблюдаться вялость, атаксия, тремор мышц, рвота и/или диарея, в этих случаях применение препарата прекращают и животному назначают средства симптоматической терапии.', 'Количество штук в упаковке': '2 таблетки', 'Срок годности (дни)': '730', 'Вес (гр)': '50', 'Ширина (мм)': '60', 'Длина (мм)': '120', 'Высота (мм)': '30', 'Страна-производитель': 'Франция', 'Бренд': 'Мильбемакс', 'Варианты': ''}, 'price': '749.0'}
# Traceback (most recent call last):
#   File "D:\Lemurrr\venv\lib\site-packages\postgrest\_sync\request_builder.py", line 70, in execute
#     raise APIError(r.json())
#   File "D:\Lemurrr\venv\lib\site-packages\httpx\_models.py", line 756, in json
#     return jsonlib.loads(self.text, **kwargs)
#   File "C:\Users\Евгений\AppData\Local\Programs\Python\Python310\lib\json\__init__.py", line 346, in loads
#     return _default_decoder.decode(s)
#   File "C:\Users\Евгений\AppData\Local\Programs\Python\Python310\lib\json\decoder.py", line 337, in decode
#     obj, end = self.raw_decode(s, idx=_w(s, 0).end())
#   File "C:\Users\Евгений\AppData\Local\Programs\Python\Python310\lib\json\decoder.py", line 355, in raw_decode
#     raise JSONDecodeError("Expecting value", s, err.value) from None
# json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
#
# During handling of the above exception, another exception occurred:
#
# Traceback (most recent call last):
#   File "D:\Lemurrr\venv\lib\site-packages\twisted\internet\defer.py", line 892, in _runCallbacks
#     current.result = callback(  # type: ignore[misc]
#   File "D:\Lemurrr\venv\lib\site-packages\scrapy\utils\defer.py", line 340, in f
#     return deferred_from_coro(coro_f(*coro_args, **coro_kwargs))
#   File "D:\Lemurrr\lemurrr\lemurrr\pipelines.py", line 155, in process_item
#     eq('value', param_value).execute()
#   File "D:\Lemurrr\venv\lib\site-packages\postgrest\_sync\request_builder.py", line 74, in execute
#     raise APIError(generate_default_error_message(r))
# postgrest.exceptions.APIError: {'message': 'JSON could not be generated', 'code': 414, 'hint': 'Refer to full message for details', 'details': "b'URI too long\\n'"}