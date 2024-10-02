import json
import requests
import os
from tqdm import tqdm
import pandas as pd


def open_file(path):
    if path is None:
        path = 'lemurrr/lemurrr/lemurrrCards_2023-10-05T07-52-59+00-00.json'
    with open(path, encoding='utf-8') as Lf:
        links_data: object = json.load(Lf)
    return links_data


def write_file(new_obj, file_name, to_json, write_mode):
    if to_json:
        data_object = json.dumps(new_obj, ensure_ascii=False, indent=4)
    else:
        data_object = new_obj

    with open(file_name, write_mode, encoding='utf-8') as outfile:
        outfile.write(data_object)


def download(url, pathname):
    """
    Загружает файл по URL‑адресу и помещает его в папку `pathname`
    """
    # если путь не существует, сделать этот путь dir
    if not os.path.isdir(pathname):
        os.makedirs(pathname)
    # загружаем тело ответа по частям, а не сразу
    response = requests.get(url, stream=True)
    # get the total file size
    file_size = int(response.headers.get("Content-Length", 0))
    # получаем имя файла
    filename = os.path.join(pathname, url.split("/")[-1])
    # индикатор выполнения, изменение единицы измерения на байты вместо итераций (по умолчанию tqdm)
    progress = tqdm(response.iter_content(1024), f"Downloading {filename}", total=file_size, unit="B", unit_scale=True,
                    unit_divisor=1024)
    with open(filename, "wb") as f:
        for data in progress.iterable:
            # записываем прочитанные данные, в файл
            f.write(data)
            # обновить вручную индикатор выполнения
            progress.update(len(data))

    return filename


def extract_images(file_data):
    if file_data is None:
        return

    for i in tqdm(file_data, "Extracting:"):
        path = i['card_catalog'][0] + "/" + i['card_tag'][0]
        if not os.path.exists(path):
            for img_src_main in i['img_src_main']:
                if img_src_main:
                    download(('https://lemurrr.ru' + img_src_main), path)
            for card_images in i['card_images']:
                if card_images:
                    download(('https://lemurrr.ru' + card_images), path)


def convert_list_to_dict(lst):
    try:
        params = open_file('lemurrr_card_params.json')
    finally:
        print('There is no file lemurrr_card_params.json!')
    print(params)
    if not params:
        from_scratch = True
        params = []
    else:
        from_scratch = False
    no_params = ['Италия', 'Россия', 'Таиланд', 'Франция', 'Китай']
    caract_index = 0
    prev_param = False
    for caract in lst:
        if caract['card_caracter']:
            res_dct = {}
            res_dct_values = []
            for element in caract['card_caracter']:
                if from_scratch:
                    print(element, (element.strip() in params))
                    if element.strip()[0].isupper() and prev_param != True \
                            and element.strip() not in params and element.strip() not in no_params:
                        params.append(element.strip())
                        prev_param = True
                        answ = input('Is this ' + element + ' a param?')
                        if answ == '0':
                            params.remove(element.strip())
                            no_params.append(element.strip())
                            res_dct_values.append(element.strip())
                            res_dct.update({params[-1]: res_dct_values})
                            prev_param = False
                        else:
                            res_dct_values = []
                    elif element.strip() not in params:
                        res_dct_values.append(element.strip())
                        res_dct.update({params[-1]: res_dct_values})
                        prev_param = False
                    else:
                        res_dct_values = []
                        prev_param = True
                else:
                    if element.strip() in params:
                        param = element.strip()
                        res_dct_values = []
                    else:
                        res_dct_values.append(element.strip())
                        res_dct.update({param: res_dct_values})

            caract.update({'card_caracter': res_dct})
            lst[caract_index] = caract
            print(lst[caract_index])
            caract_index += 1
            # input('Next?')
    return lst


def convert_data_to_yml_old(data, in_store):
    data_str = '<?xml version="1.0" encoding="UTF-8"?><yml_catalog date="2019-11-01 17:22"><shop>' \
               '<name>Зоомаркет Лемуррр Краснотурьинск</name><company>ИП Сухарников Е.А.</company>' \
               '<url></url><platform></platform><version>1.0</version><agency></agency>' \
               '<email>ovr58armee@yandex.ru</email><currencies><currency id="RUR" rate="1"/></currencies><categories>'
    write_file(data_str, 'lemurrr_cards_with_params.xml', False, 'a')
    categories = []
    sub_categories = []
    for data_item in data:
        if data_item['card_crumbs'][0] not in categories:
            categories.append(data_item['card_crumbs'][0])
            parentId = categories.index(data_item['card_crumbs'][0])
        else:
            parentId = categories.index(data_item['card_crumbs'][0])
        if len(data_item['card_crumbs']) > 1:
            if {str(parentId): data_item['card_crumbs'][1]} not in sub_categories:
                sub_categories.append({str(parentId): data_item['card_crumbs'][1]})
    data_str1 = ''
    for category_item in tqdm(categories, 'Categorising...'):
        data_str1 = data_str1 + '<category id=\"' + str(categories.index(category_item)) + '\">' + \
                    category_item + '</category>'
        sub_category_items = [d.get(str(categories.index(category_item)), None) for d in sub_categories]
        for sub_category_item in sub_category_items:
            if sub_category_item is not None:
                data_str1 = data_str1 + '<category id=\"1' + str(categories.index(category_item)) + \
                            str(sub_category_items.index(sub_category_item)) + '\" parentId=\"' + \
                            str(categories.index(category_item)) + '\">' + sub_category_item + '</category>'
    data_str1 += '</categories><offers>'
    write_file(data_str1, 'lemurrr_cards_with_params.xml', False, 'a')
    for data_item in tqdm(data, 'Formating...'):
        data_str2 = ''
        if in_store and data_item['card_tag'][0] in in_store.keys():
            data_str2 = data_str2 + '<offer id=\"' + str(data_item['card_tag'][0]) + '\">'
            data_str2 = data_str2 + '<name>' + str(data_item['card_title'][0]) + '</name>'
            if data_item['card_catalog']:
                data_str2 = data_str2 + '<vendor>' + str(data_item['card_catalog'][0].split('_')[-1]) + '</vendor>'
            if data_item['card_price_nodisc']:
                data_str2 = data_str2 + '<price>' + str(in_store[data_item['card_tag'][0]][1]) + '</price>'
            else:
                data_str2 = data_str2 + '<price>' + str(in_store[data_item['card_tag'][0]][1]) + '</price>'
            data_str2 = data_str2 + '<currencyId>RUR</currencyId>'
            if len(data_item['card_crumbs']) > 1:
                sub_category_items = [d.get(str(categories.index(data_item['card_crumbs'][0])), None)
                                      for d in sub_categories]
                data_str2 = data_str2 + '<categoryId>' + '1' + str(categories.index(data_item['card_crumbs'][0])) + str(
                    sub_category_items.index(data_item['card_crumbs'][1])) + '</categoryId>'
            else:
                data_str2 = data_str2 + '<categoryId>' + str(
                    categories.index(data_item['card_crumbs'][0])) + '</categoryId>'
            if 'img_src_main' in data_item.keys():
                if len(data_item['img_src_main']) > 0:
                    data_str2 = data_str2 + '<picture>https://lemurrr.ru' + str(
                        data_item['img_src_main'][0]) + '</picture>'
            if len(data_item['card_images']) > 0:
                data_str2 = data_str2 + str(''.join([
                    ('<picture>https://lemurrr.ru' + imgs + '</picture>') for imgs in data_item[
                        'card_images']]))
            data_str2 += '<delivery>true</delivery><pickup>true</pickup><store>true</store><description>'
            if len(data_item['card_description']) > 0:
                text_description = str(' '.join(txt for txt in data_item['card_description']))[:2987]
                text_description += '...' * (len(text_description) > 2987)
                text_description = '<p>' + text_description + '</p>'
            elif len(data_item['card_info']) > 0:
                text_description = str(' '.join(txt for txt in data_item['card_info']))[:2987]
                text_description += '...' * (len(text_description) > 2987)
                text_description = '<p>' + text_description + '</p>'
            elif len(data_item['card_composition']) > 0:
                text_description = str(' '.join(txt for txt in data_item['card_composition']))[:2987]
                text_description += '...' * (len(text_description) > 2987)
                text_description = '<p>' + text_description + '</p>'
            elif len(data_item['card_caracter']) > 0:
                text_description = '<table><thead><tr><th>Характеристика</th><th>Значение</th></tr></thead><tbody>'
                for caracter_text in data_item['card_caracter'].keys():
                    text_description += '<tr><td>' + str(caracter_text) + '</td><td>' + str(
                        ' '.join(data_item['card_caracter'][caracter_text])) + '</td></tr>'
                text_description += '</tbody></table>'
            else:
                text_description = data_item['card_title'][0]

            data_str2 += '<![CDATA[' + text_description + ']]></description> '

            data_str2 += '<sales_notes>Необходима предоплата.</sales_notes>'
            if 'Страна-производитель' in data_item['card_caracter'].keys():
                if len(data_item['card_caracter']['Страна-производитель']) > 0:
                    data_str2 += '<country_of_origin>' + ','.join(data_item['card_caracter']['Страна-производитель']) + \
                                 '</country_of_origin>'

            param_text = ''

            if len(data_item['card_caracter']) > 0:

                for param_name in data_item['card_caracter'].keys():
                    if '(' in param_name:
                        unit_txt = param_name[param_name.find("(") + 1:param_name.find(")")]
                        param_txt = param_name.split('(')[0]
                        param_text += '<param name=\"' + param_txt.strip() + '\" unit=\"' + unit_txt + '\">' + str(
                            ', '.join(data_item['card_caracter'][param_name])) + '</param>'
                    else:
                        param_text += '<param name=\"' + param_name.strip() + '\">' + str(
                            ', '.join(data_item['card_caracter'][param_name])) + '</param>'
            data_str2 += param_text
            weight_kg = 0
            if 'Вес (гр)' in data_item['card_caracter'].keys():
                if len(data_item['card_caracter']['Вес (гр)']) > 0:
                    if ' ' in data_item['card_caracter']['Вес (гр)'][0]:
                        weight_kg = float(data_item['card_caracter']['Вес (гр)'][0].split('-')[-1].replace('к', '').
                                          replace('г', '').replace('р', '')) / 1000
                    else:
                        weight_kg = float(data_item['card_caracter']['Вес (гр)'][0].split('-')[-1].replace('к', '').
                                          replace('г', '').replace('р', '').replace(',', '.')) / 1000
            elif 'Вес упаковки' in data_item['card_caracter'].keys():
                if len(data_item['card_caracter']['Вес упаковки']) > 0:
                    if 'г' in data_item['card_caracter']['Вес упаковки'][0]:
                        weight_kg = float(data_item['card_caracter']['Вес упаковки'][0].split(' ')[0].replace('к', '').
                                          replace('г', '').replace('р', '').replace(',', '.')) / 1000
                    elif 'к' in data_item['card_caracter']['Вес упаковки'][0]:
                        weight_kg = float(data_item['card_caracter']['Вес упаковки'][0].split(' ')[0].replace('к', '').
                                          replace('г', '').replace('р', '').replace(',', '.'))
                    else:
                        weight_kg = float(data_item['card_caracter']['Вес упаковки'][0].replace(',', '.'))
            elif ',* кг' in data_item['card_title'][0]:
                weight_kg = float(data_item['card_title'][0].split(' ')[-2].replace(',', '.'))
            elif ',* гр' in data_item['card_title'][0]:
                weight_kg = float(data_item['card_title'][0].split(' ')[-2].replace(',', '.')) / 1000

            data_str2 += '<weight>' + str(weight_kg) + '</weight>'

            if 'Ширина (мм)' in data_item['card_caracter'].keys() and \
                    'Длина (мм)' in data_item['card_caracter'].keys() and \
                    'Высота (мм)' in data_item['card_caracter'].keys():
                dementions_txt = str(float(data_item['card_caracter']['Длина (мм)'][0].split('-')[-1].
                                           replace(',', '.').replace('м', '').replace(' с', '0')) / 10) + '/' + \
                                 str(float(data_item['card_caracter']['Ширина (мм)'][0].split('-')[-1].
                                           replace(',', '.').replace('м', '').replace(' с', '0')) / 10) + '/' + \
                                 str(float(data_item['card_caracter']['Высота (мм)'][0].split('-')[-1].
                                           replace(',', '.').replace('м', '').replace(' с', '0')) / 10)

                data_str2 += '<dimensions>' + dementions_txt + '</dimensions>'

            data_str2 += '<count>' + str(in_store[data_item['card_tag'][0]][0]) + '</count>'

            data_str2 += '</offer>'
            write_file(data_str2, 'lemurrr_cards_with_params.xml', False, 'a')

    data_str3 = '</offers></shop></yml_catalog>'
    write_file(data_str3, 'lemurrr_cards_with_params.xml', False, 'a')
    return print('Done...')


def convert_xls_to_data(storage_file):
    storage_data = pd.read_excel(storage_file, index_col=None, header=0,
                                 dtype={'Склад': str, 'Наименование': str, 'Остаток': int, 'Цена': float})
    storage_data['Остаток'] = [list(x) for x in zip(storage_data['Остаток'].tolist(), storage_data['Цена'].tolist())]

    in_store = storage_data.set_index('Склад').to_dict()['Остаток']

    return in_store


def to_param_csv(data, csv_file_name):
    df = pd.read_json(data)
    df.to_excel(csv_file_name)


def generate_id(from_name):
    alphabet = {' ': '0', 'а': '1', 'б': '2', 'в': '3', 'г': '4', 'д': '5', 'е': '6', 'ё': '7', 'ж': '8', 'з': '9',
                'и': 'A', 'к': 'B', 'л': 'C', 'м': 'D', 'н': 'C', 'о': 'E', 'п': 'F', 'р': 'G',
                'с': 'H', 'т': 'I', 'у': 'J', 'ф': 'K', 'х': 'L', 'ц': 'M', 'ч': 'N', 'ш': 'O',
                'щ': 'P', 'ъ': 'Q', 'ы': 'R', 'ь': 'S', 'э': 'T', 'ю': 'U', 'я': 'V'}

    id_from_name = ''.join(alphabet[y] for y in from_name.lower())

    return id_from_name


print(generate_id('Аквариумы для рыб'))
# file_data = open_file('lemurrr_cards_with_params.json')

# new_dct = convert_list_to_dict(file_data)

# write_file(new_dct, 'lemurrr_cards_with_params.json')

# print(convert_xls_to_data('storage121023.xlsx'))

# for item in file_data:
#     item['card_crumbs']
#     # new_dict = {k: v for k, v in item.items() if 'caracter' in k}
#     print(item['card_caracter'].keys())
