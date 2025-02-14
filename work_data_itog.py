import numpy as np
import pandas as pd
pd.set_option("expand_frame_repr", False)
pd.set_option('display.max_colwidth', None)
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import csv
class WorkForData:
    def __init__(self):
        # Подбираем из окружения данные
        load_dotenv()
        # Записываем в переменные
        self.host = os.getenv('HOST')
        self.port = os.getenv('PORT')
        self.database = os.getenv('DATABASE_NAME')
        self.user = os.getenv('LOGIN')
        self.password = os.getenv('PASS')
        self.conn_to_db = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        # Получаем данные о магазинах
        print('Получаем данные о магазинах! К-к-арамба!')
        self.store = self.__take_data_for_DB(['search_store','id_store_rename'], 'spr_store_rename')
        print('Получили данные с магазинов! К-к-арамба!')
        # Получаем данные о продуктах
        print('Получаем данные о продуктах! К-к-арамба!')
        self.product = self.__take_data_for_DB(['id_product_code', 'id_product'], 'spr_product')
        print('Получили данные с пробуктов! К-к-арамба!')
        # Путь до папки с нашими файлами
        self.path_to_directory = '2024_new'
        # Путь до старого формата
        self.path_to_directory_old = '2023'

    def first_start(self):
        print('Start!')
        # Работа с новым форматом
        self.__take_all_file()
        # Работа со старым форматом
        self.__take_all_old_file()
    def __take_all_file(self):
        """Получаем список файлов с папки и прогоняем их все по соединению с другими данными. Куда сохранять не знаю"""
        except_file_path = 'except.csv'  # Замените на фактический путь к except.csv

        # Считываем имена файлов из except.csv
        try:
            except_df = pd.read_csv(except_file_path)
            # Предполагаем, что в except.csv есть один столбец с именами файлов
            excluded_files = except_df.iloc[:, 0].tolist()  # Получаем первый столбец как список
        except FileNotFoundError:
            print(f"Файл '{except_file_path}' не найден.")
            excluded_files = []  # Если файл не найден, создаем пустой список

        # Список для хранения файлов .txt
        txt_files = []

        # Проходим по всем файлам в директории
        for filename in os.listdir(self.path_to_directory):
            # Проверяем, если файл имеет расширение .txt
            if filename.endswith('.txt'):
                # Проверяем, содержится ли имя файла в списке исключений
                if filename not in excluded_files:
                    # Добавляем файл в список
                    txt_files.append(filename)

        # Добавляю переменные для отслеживания результатов работы
        len_txt_files = len(txt_files)
        if len_txt_files == 0:
            print('Нет данных')
            quit()
        counter = 0
        print('New data')
        for file in txt_files:
            print(f"Работа с файлом {file}")
            # Получаем данные по файлу
            df = self.__take_data_for_file(self.path_to_directory + '/' + file)
            # Вызываем функцию и получаем датасет со столбцами id_product и id_store_rename
            df = self.__full_id_store_merge(df)
            #ПРОБЛЕМЫ С ДАННЫМИ С БД
            df_err = df[df['id_store_rename'] == 0]['Магазин'].unique()
            df = df.drop(
                ['Магазин'], axis=1)
            self.__error_data(df_err)

            # Добавляем дату
            try:
                # Код, который может вызвать исключение
                df['order_date'] = pd.to_datetime(file.rsplit('.', 1)[0], format='%d.%m.%Y')
            except ValueError as e:
                df['order_date'] = pd.to_datetime(file.rsplit('.', 1)[0], format='%d.%m.%y')
            # Заполняем пустые строки нулями
            df['Начальный остаток себестоимость'] = df['Начальный остаток себестоимость'].fillna(0)
            df['Начальный остаток'] = df['Начальный остаток'].fillna(0)
            df['Конечный остаток себестоимость'] = df['Конечный остаток себестоимость'].fillna(0)
            df['Конечный остаток'] = df['Конечный остаток'].fillna(0)
            # Типо вывод

            counter +=1
            print(f"{counter}/ {len_txt_files}")

            self.__add_except(file)
            self.__data_to_DB(df)
        #return df
    def __take_all_old_file(self):
        """Получаем список файлов с папки и прогоняем их все по соединению с другими данными. Куда сохранять не знаю"""
        # Список для хранения имен файлов
        except_file_path = 'except.csv'  # Замените на фактический путь к except.csv

        # Считываем имена файлов из except.csv
        try:
            except_df = pd.read_csv(except_file_path)
            # Предполагаем, что в except.csv есть один столбец с именами файлов
            excluded_files = except_df.iloc[:, 0].tolist()  # Получаем первый столбец как список
        except FileNotFoundError:
            print(f"Файл '{except_file_path}' не найден.")
            excluded_files = []  # Если файл не найден, создаем пустой список

        # Список для хранения файлов .txt
        txt_files = []

        # Проходим по всем файлам в директории
        for filename in os.listdir(self.path_to_directory_old):
            # Проверяем, если файл имеет расширение .txt
            if filename.endswith('.txt'):
                # Проверяем, содержится ли имя файла в списке исключений
                if filename not in excluded_files:
                    # Добавляем файл в список
                    txt_files.append(filename)
        len_txt_files = len(txt_files)
        counter = 0
        print('Old data')
        for file in txt_files:
            print(f'Работа с файлом {file}')
            df = self.__take_data_for_file_old(self.path_to_directory_old + '/' +file)
            df = self.__full_id_store_merge_old(df)
            df_err = df[df['id_store_rename'] == 0]['Магазин'].unique()
            df = df.drop(
                ['Магазин'], axis=1)
            self.__error_data(df_err)

            # Добавляем дату
            df.rename(columns={
                'По дням': 'order_date',
            }, inplace=True)
            try:
                # Код, который может вызвать исключение
                df['order_date'] = pd.to_datetime(file.rsplit('.', 1)[0], format='%d.%m.%Y')
            except ValueError as e:
                df['order_date'] = pd.to_datetime(file.rsplit('.', 1)[0], format='%d.%m.%y')
            # Заполняем пустые строки нулями
            df['Начальный остаток себестоимость'] = df['Начальный остаток себестоимость'].fillna(0)
            df['Начальный остаток'] = df['Начальный остаток'].fillna(0)
            df['Конечный остаток себестоимость'] = df['Конечный остаток себестоимость'].fillna(0)
            df['Конечный остаток'] = df['Конечный остаток'].fillna(0)
            df['id_product'] = pd.to_numeric(df['id_product'], errors='coerce').astype(
                'Int64')  # Используйте 'Int64' для поддержки NaN
            df['id_store_rename'] = pd.to_numeric(df['id_store_rename'], errors='coerce').astype('Int64')
            # Типо вывод

            counter += 1
            print(f"{counter}/ {len_txt_files}")
            self.__add_except(file)
            self.__data_to_DB(df)
    def __data_to_DB(self, df):
        """Это типо подключение к бд"""
        print('QQQ')
        #print(df)
        # Создаём подключение
        #engine = create_engine(self.conn_to_db)
        # Загружаем в базу данных
        #df.to_sql('table_name', engine, if_exists='replace', index=False)
    def __take_data_for_file(self, file_path):
        """Получаем данные с файла"""
        # Столбцы, которые будут в датасете
        column_names = [
            'Организация',
            'Магазин',
            'Номер магазина',
            'Номенклатура.Код',
            'Начальный остаток',
            'Начальный остаток себестоимость',
            'Конечный остаток',
            'Конечный остаток себестоимость'
        ]

        # Считываем данные из файла, пропуская первые 8 строк
        df = pd.read_csv(file_path, sep='\t', skiprows=8, names=column_names, encoding='utf-8')
        # Убираем последнюю, где Итог
        df = df.iloc[:-1]
        # Преобразование столбцов в численные выражения. Первые две это Организация и Магазин. Их пропускаем, как и Номенклатуру
        for column in column_names[2:]:
            if column != 'Номенклатура.Код':
                df[column] = self.__column_to_float(df, column)
        return df
    def __take_data_for_file_old(self, file_path):
        """Получаем данные с файла"""
        # Столбцы, которые будут в датасете
        column_names = [
            'Организация', "Магазин",
            'Номер магазина', 'Номенклатура', 'Код', 'Магазин.Номер магазина', 'По дням',
            'Начальный остаток', 'Начальный остаток себестоимость', 'Конечный остаток',
            'Конечный остаток себестоимость'
        ]

        # Считываем данные из файла, пропуская первые 8 строк
        df = pd.read_csv(file_path, sep='\t', skiprows=8, names=column_names, encoding='utf-8', skipinitialspace=True)

        # Убираем последнюю, где Итог
        df = df.dropna(subset=['Номенклатура'], how='all')

        # Преобразование столбцов в численные выражения. Первые две это Организация и Магазин. Их пропускаем, как и Номенклатуру
        for column in column_names[2:]:
            if column not in ['Номенклатура', 'Код', 'По дням']:
                df[column] = self.__column_to_float(df, column)
        return df
    def __column_to_float(self, df, column_name):
        """Необходимо для преобразования столбцов в int или float"""
        # Убираем пробелы и табуляцию между числами

        df[column_name] = df[column_name].str.replace('\xa0', '').str.replace(' ', '')

        # Если число с остатком меняем , на .. Пример: 1,000 в 1.000
        df[column_name] = df[column_name].str.replace(',', '.')
        # Преобразуем данные в численные
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
        # Проверка, чтобы Номер магазины был целочисленным
        return df[column_name]
    def __full_id_store_merge(self, df):
        """В ф-ии соединяем три датасета: продукты(Номенклатура), магазины(Названия) и наши данные"""
        # Соединяем с продуктами
        merged_df = pd.merge(df, self.product, left_on='Номенклатура.Код', right_on='id_product_code', how='left')


        # Соединяем с магазинами

        merged_df = pd.merge(merged_df, self.store, left_on='Магазин', right_on='search_store', how='left')
        # Заполняем пустые значения. Без этого не сможем преобразовать в целочисленный столбец
        merged_df['id_store_rename'] = merged_df['id_store_rename'].fillna(0)
        # Преобразуем в тип int
        merged_df['id_store_rename'] = merged_df['id_store_rename'].astype(int)

        # Этих двух магазинов нет в БД, поэтому вручную заполняю данные
        merged_df = merged_df[merged_df['Магазин'] != 'ФРС Центральный офис']
        merged_df = merged_df[merged_df['Магазин'] != 'Служебный магазин']
        merged_df = merged_df[merged_df['Магазин'] != 'Служебный магазин ООО Волков']
        merged_df = merged_df[merged_df['Магазин'] != 'Лаборатория Контроль КРСК']
        merged_df = merged_df[merged_df['Магазин'] != 'Лаборатория Контроль']
        #merged_df = merged_df[merged_df['Магазин'] != 'Вендинг Новокузнецк, Димитрова ул, 33']
        #merged_df = merged_df[merged_df['Магазин'] != 'Микромаркет Красноярск, 78 Добровольческой бригады ул, 15 пом.22']

        #merged_df.loc[merged_df['Магазин'] == 'ФР А-Судженск, 50-летия Октября ул, 1', 'id_store_rename'] = 42036
        #merged_df.loc[merged_df['Магазин'] == 'ФР Новосибирск, Каменская ул, 44 (ПОН)', 'id_store_rename'] = 54006
        #merged_df.loc[merged_df['Магазин'] == 'ФР Кемерово, Терешковой ул, 22а (РАС)', 'id_store_rename'] = 42018
        #merged_df.loc[merged_df['Магазин'] == 'ФР Новосибирск, Вокзальная магистраль ул, 4а', 'id_store_rename'] = 54100
        merged_df = merged_df[~merged_df['Магазин'].str.contains('Вендинг|Микромаркет', na=False)]
        # Если их надо исключить, то вот код
        """
        # Этих двух магазинов нет в БД, поэтому вручную заполняю данные
        merged_df.loc[merged_df['Магазин'] == 'ФРС Центральный офис', 'id_store_rename'] = 42999
        merged_df.loc[merged_df['Магазин'] == 'Служебный магазин', 'id_store_rename'] = 42001

        """
        # Заполняем пустые значения. Без этого не сможем преобразовать в целочисленный столбец
        merged_df['id_product'] = merged_df['id_product'].fillna(0)
        merged_df['id_product'] = merged_df['id_product'].astype(int)
        # Создаём датасет с ошибками, где пустые строки или мало данным
        df_error = merged_df[merged_df['id_product'] == 0]['Номенклатура.Код']
        try:
            existing_data = pd.read_csv('error_Номенклатура.csv')
        except FileNotFoundError:
            # Если файл не существует, создаем пустой DataFrame
            existing_data = pd.DataFrame(columns=["Код"])

        # Преобразуем df_error в DataFrame с именем столбца "Код"
        df_error_df = df_error.reset_index(drop=True).to_frame(name="Код")

        # Объединяем существующие данные с новыми
        combined_data = pd.concat([existing_data, df_error_df], ignore_index=True)

        # Удаляем дубликаты (по всем столбцам)
        combined_data = combined_data.drop_duplicates()

        # Записываем обновлённые данные обратно в CSV-файл
        combined_data.to_csv('error_Номенклатура.csv', index=False, encoding='utf-8')
        #print(merged_df[merged_df['id_product'] == 0])

        # Убираем лишние столбцы
        try:
            merged_df = merged_df.drop(['Номер магазина','search_store' , 'Номенклатура.Код' ,'Организация', 'id_product_code'], axis=1)
        except KeyError:
            merged_df = merged_df.drop(
                ['Номер магазина', 'search_store', 'Код', 'Организация', 'id_product_code'],
                axis=1)

        return merged_df
    def __full_id_store_merge_old(self, df):
        """В ф-ии соединяем три датасета: продукты(Номенклатура), магазины(Названия) и наши данные"""
        # Соединяем с продуктами

        merged_df = pd.merge(df, self.product, left_on='Код', right_on='id_product_code', how='left')

        # Соединяем с магазинами
        merged_df = pd.merge(merged_df, self.store, left_on='Магазин', right_on='search_store', how='left')

        # Заполняем пустые значения. Без этого не сможем преобразовать в целочисленный столбец
        merged_df['id_store_rename'] = merged_df['id_store_rename'].fillna(0)
        # Преобразуем в тип int
        merged_df['id_store_rename'] = merged_df['id_store_rename'].astype(int)

        # Если их надо исключить, то вот код
        merged_df = merged_df[merged_df['Магазин'] != 'ФРС Центральный офис']
        merged_df = merged_df[merged_df['Магазин'] != 'Служебный магазин']
        merged_df = merged_df[merged_df['Магазин'] != 'Служебный магазин ООО Волков']
        merged_df = merged_df[merged_df['Магазин'] != 'Лаборатория Контроль КРСК']
        merged_df = merged_df[merged_df['Магазин'] != 'Лаборатория Контроль']
        #merged_df = merged_df[merged_df['Магазин'] != 'Вендинг Новокузнецк, Димитрова ул, 33']
        #merged_df = merged_df[merged_df['Магазин'] != 'Микромаркет Красноярск, 78 Добровольческой бригады ул, 15 пом.22']

        #merged_df.loc[merged_df['Магазин'] == 'ФР А-Судженск, 50-летия Октября ул, 1', 'id_store_rename'] = 42036
        #merged_df.loc[merged_df['Магазин'] == 'ФР Новосибирск, Каменская ул, 44 (ПОН)', 'id_store_rename'] = 54006
        #merged_df.loc[merged_df['Магазин'] == 'ФР Кемерово, Терешковой ул, 22а (РАС)', 'id_store_rename'] = 42018
        #merged_df.loc[merged_df['Магазин'] == 'ФР Новосибирск, Вокзальная магистраль ул, 4а', 'id_store_rename'] = 54100

        #Если нам не нужны вендинги и Микромаркеты
        merged_df = merged_df[~merged_df['Магазин'].str.contains('Вендинг|Микромаркет', na=False)]

        """
                # Этих двух магазинов нет в БД, поэтому вручную заполняю данные
        merged_df.loc[merged_df['Магазин'] == 'ФРС Центральный офис', 'id_store_rename'] = 42999
        merged_df.loc[merged_df['Магазин'] == 'Служебный магазин', 'id_store_rename'] = 42001

        """
        # Заполняем пустые значения. Без этого не сможем преобразовать в целочисленный столбец
        merged_df['id_product'] = merged_df['id_product'].fillna(0)
        # Этих двух магазинов нет в БД, поэтому вручную заполняю данные
        merged_df['id_product'] = merged_df['id_product'].astype(int)
        # Создаём датасет с ошибками, где пустые строки или мало данным
        df_error = merged_df[merged_df['id_product'] == 0]['Код']
        try:
            existing_data = pd.read_csv('error_Номенклатура_2.csv')
        except FileNotFoundError:
            # Если файл не существует, создаем пустой DataFrame
            existing_data = pd.DataFrame(columns=["Код"])

        # Преобразуем df_error в DataFrame с именем столбца "Код"
        df_error_df = df_error.reset_index(drop=True).to_frame(name="Код")

        # Объединяем существующие данные с новыми
        combined_data = pd.concat([existing_data, df_error_df], ignore_index=True)

        # Удаляем дубликаты (по всем столбцам)
        combined_data = combined_data.drop_duplicates()

        # Записываем обновлённые данные обратно в CSV-файл
        combined_data.to_csv('error_Номенклатура_2.csv', index=False, encoding='utf-8')


        # Убираем лишние столбцы
        merged_df = merged_df.drop(['Номенклатура','Магазин.Номер магазина','Номер магазина', 'search_store',  'Номенклатура' ,'Организация', 'id_product_code',
                                    'Код'], axis=1)


        return merged_df
    def __take_data_for_DB(self, columns_list, table):
        """Получаем данные с БД. В columns_list должны быть сначала str столбец, а потом id"""
        # Создаем строку подключения
        connection_string = self.conn_to_db
        # Создаём объект Engine
        engine = create_engine(connection_string)
        # SQL-запрос для извлечения данных
        query = f"""SELECT {columns_list[0]}, {columns_list[1]}
        FROM {table}"""
        # Выполняем запрос и данные от него в датафрейм
        df = pd.read_sql_query(query, engine)
        # Завершаем подключение к БД
        engine.dispose()
        # Объявляем столбцы с ID целочисленными
        df[columns_list[1]] = df[columns_list[1]].fillna(0)
        df[columns_list[1]] = df[columns_list[1]].astype(int)
        return df
    def __error_data(self, df_err):
        try:

            # Путь к вашему текстовому файлу
            file_path = 'NON_DB_ERROR.txt'  # Замените на фактический путь

            # Считываем существующие строки из файла в множество
            existing_lines = set()
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as file:
                    existing_lines = set(list(file.read().splitlines()))

            # Создаем пустое множество для хранения новых уникальных строк
            new_unique_lines = set()
            line = '; '.join(df_err)

            # Проверяем, уникальна ли строка
            if line not in existing_lines:
                new_unique_lines.add(line)

            # Открываем файл в режиме добавления
            with open(file_path, 'a', encoding='utf-8') as file:
                for line in new_unique_lines:
                    file.write(line + '\n')

            # Создаем пустое множество для хранения уникальных значений
            unique_values = set()

            # Чтение данных из текстового файла
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    # Убираем пробелы и разбиваем строку по запятой
                    addresses = line.split(';')
                    # Добавляем адреса в множество для уникальности
                    unique_values.update(addresses)

            # Запись уникальных значений обратно в текстовый файл
            with open(file_path, 'w', encoding='utf-8') as file:
                for value in unique_values:
                    file.write(value + '\n')
        except TypeError as e:
                print("Возможно пустое значение")
                print(df_err)
                print(f"Вышла ошибочка: {e}")
    def __add_except(self, filename):
        file_path = 'except.csv'  # Замените на фактический путь

        # Считывание данных из CSV-файла
        try:
            df = pd.read_csv(file_path)
        except FileNotFoundError:
            print(f"Файл '{file_path}' не найден.")
            df = pd.DataFrame(columns=['Исключения'])



        # Создание DataFrame для новой строки
        new_row = pd.DataFrame({
            'Исключения': [filename]
        })

        # Объединение DataFrames
        df = pd.concat([df, new_row], ignore_index=True)

        # Запись обратно в CSV-файл
        df.to_csv(file_path, index=False)

        print("Данные успешно обновлены и сохранены.")