import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import numpy as np

# Конфигурация подключения к БД
DB_CONFIG = {
    'host': 'localhost',
    'database': 'real_estate_moscow',
    'user': 'login', # замени на свой логин
    'password': 'pwd',  # замени на свой пароль
    'port': '5432'
}

def load_csv_to_db(csv_file='comprehensive_real_estate_dataset.csv'):
    """Загружает данные из CSV в базу данных"""
    
    print("Загрузка данных из CSV...")
    df = pd.read_csv(csv_file, encoding='utf-8')
    df['publish_date'] = pd.to_datetime(df['publish_date'])
    print(f"Загружено {len(df)} строк")
    
    # Подключаемся к БД
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("Подключено к базе данных")
    
    # Загружаем справочники
    print("Загрузка справочников...")
    
    # Районы
    cursor.execute("SELECT district_name, district_id FROM dim_districts")
    districts = {name: id for name, id in cursor.fetchall()}
    
    # Типы недвижимости
    cursor.execute("SELECT property_type_name, property_type_id FROM dim_property_types")
    property_types = {name: id for name, id in cursor.fetchall()}
    
    # Типы домов
    cursor.execute("SELECT house_type_name, house_type_id FROM dim_house_types")
    house_types = {name: id for name, id in cursor.fetchall()}
    
    # Даты
    cursor.execute("SELECT full_date, date_id FROM dim_time")
    dates = {date: id for date, id in cursor.fetchall()}
    
    # Подготавливаем данные для вставки
    data_to_insert = []
    
    for _, row in df.iterrows():
        # Получаем ID справочников
        district_id = districts.get(row['district'])
        property_type_id = property_types.get(row['property_type'])
        
        # Тип дома (если есть)
        house_type_id = house_types.get(row.get('house_type')) if 'house_type' in row and pd.notna(row.get('house_type')) else None
        
        # Дата публикации
        publish_date = row['publish_date'].date()
        date_id = dates.get(publish_date)
        
        # Пропускаем строку если нет обязательных данных
        if not all([district_id, property_type_id, date_id]):
            print(f"Пропущена строка: нет справочника для {row['district']}, {row['property_type']} или {publish_date}")
            continue
        
        # Обрабатываем поле area - не может быть NULL в БД
        area = row['area']
        if pd.isna(area):
            # Для машиномест ставим 0, для других - 1
            if row['property_type'] == 'машиноместо':
                area = 0.0
            else:
                area = 1.0
        
        # Обрабатываем price_per_sqm
        price_per_sqm = row['price_per_sqm']
        if pd.isna(price_per_sqm):
            if area > 0:
                price_per_sqm = float(row['price']) / area if row['price'] > 0 else 0
            else:
                price_per_sqm = 0
        
        # Подготавливаем кортеж для вставки
        data = (
            district_id,
            date_id,
            property_type_id,
            house_type_id,
            None,  # commercial_purpose_id
            
            # Основные метрики
            float(row['price']) if pd.notna(row['price']) else 0.0,
            float(area),  # Уже обработано выше
            float(price_per_sqm),  # Уже обработано выше
            
            # Параметры жилой недвижимости
            int(row['rooms']) if 'rooms' in row and pd.notna(row['rooms']) else None,
            int(row['floor']) if 'floor' in row and pd.notna(row['floor']) else None,
            int(row['total_floors']) if 'total_floors' in row and pd.notna(row['total_floors']) else None,
            int(row['year_built']) if 'year_built' in row and pd.notna(row['year_built']) else None,
            
            # Параметры коммерческой недвижимости
            float(row['ceiling_height']) if 'ceiling_height' in row and pd.notna(row['ceiling_height']) else None,
            bool(row['has_ventilation']) if 'has_ventilation' in row and pd.notna(row['has_ventilation']) else None,
            bool(row['has_air_conditioning']) if 'has_air_conditioning' in row and pd.notna(row['has_air_conditioning']) else None,
            int(row['parking_spaces']) if 'parking_spaces' in row and pd.notna(row['parking_spaces']) else None,
            float(row['land_area']) if 'land_area' in row and pd.notna(row['land_area']) else None,
            
            # Общие параметры
            int(row['metro_time']) if 'metro_time' in row and pd.notna(row['metro_time']) else None,
            bool(row['has_elevator']) if 'has_elevator' in row and pd.notna(row['has_elevator']) else None,
            bool(row['is_renovated']) if 'is_renovated' in row and pd.notna(row['is_renovated']) else None,
            
            # Технические поля
            'avito',
            row['id'] if 'id' in row else None,
            row['address'] if 'address' in row and pd.notna(row['address']) else None,
            row['url'] if 'url' in row and pd.notna(row['url']) else None,
            row['publish_date'].date()
        )
        
        data_to_insert.append(data)
    
    # SQL запрос для вставки
    insert_query = """
        INSERT INTO fact_real_estate (
            district_id, date_id, property_type_id, house_type_id, commercial_purpose_id,
            price, area, price_per_sqm, rooms, floor, total_floors, year_built,
            ceiling_height, has_ventilation, has_air_conditioning, parking_spaces, land_area,
            metro_time, has_elevator, is_renovated, data_source, external_id, address, url, created_date
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (external_id) DO UPDATE SET
            price = EXCLUDED.price,
            area = EXCLUDED.area,
            price_per_sqm = EXCLUDED.price_per_sqm,
            updated_date = CURRENT_TIMESTAMP
    """
    
    # Вставляем данные пачками
    print(f"Вставляем {len(data_to_insert)} записей в БД...")
    
    try:
        # Разбиваем на пачки по 100 записей
        batch_size = 100
        for i in range(0, len(data_to_insert), batch_size):
            batch = data_to_insert[i:i + batch_size]
            execute_batch(cursor, insert_query, batch)
            conn.commit()
            print(f"Вставлено {i + len(batch)} из {len(data_to_insert)} записей")
        
        print("Данные успешно загружены!")
        
        # Показываем статистику
        cursor.execute("SELECT COUNT(*) FROM fact_real_estate")
        total = cursor.fetchone()[0]
        print(f"Всего записей в БД: {total}")
        
        # Статистика по типам
        cursor.execute("""
            SELECT pt.property_type_name, COUNT(*) 
            FROM fact_real_estate f 
            JOIN dim_property_types pt ON f.property_type_id = pt.property_type_id 
            GROUP BY pt.property_type_name 
            ORDER BY COUNT(*) DESC
        """)
        print("\nРаспределение по типам недвижимости:")
        for prop_type, count in cursor.fetchall():
            print(f"  {prop_type}: {count}")
            
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при вставке данных: {e}")
        raise
    
    finally:
        cursor.close()
        conn.close()

def check_db_connection():
    """Проверяет подключение к БД"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()
        print(f"PostgreSQL версия: {version[0]}")
        
        # Проверяем, что таблица fact_real_estate существует
        cursor.execute("""
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = 'fact_real_estate'
            ORDER BY ordinal_position
        """)
        
        print("\nСтруктура таблицы fact_real_estate:")
        for col_name, data_type, is_nullable in cursor.fetchall():
            print(f"  {col_name}: {data_type} ({'NULL' if is_nullable == 'YES' else 'NOT NULL'})")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Ошибка подключения: {e}")
        return False

def fix_csv_data(csv_file='comprehensive_real_estate_dataset.csv'):
    """Исправляет данные в CSV перед загрузкой"""
    print("Проверка и исправление данных в CSV...")
    
    df = pd.read_csv(csv_file, encoding='utf-8')
    
    # Статистика по пропущенным значениям
    print("\nПропущенные значения:")
    for col in df.columns:
        missing = df[col].isna().sum()
        if missing > 0:
            print(f"  {col}: {missing} пропущенных")
    
    # Исправляем area для машиномест
    mask = (df['property_type'] == 'машиноместо') & (df['area'].isna())
    df.loc[mask, 'area'] = 0.0
    
    # Исправляем area для других типов с NaN
    mask = df['area'].isna()
    df.loc[mask, 'area'] = 1.0
    
    # Пересчитываем price_per_sqm если нужно
    mask = df['price_per_sqm'].isna() & (df['area'] > 0) & (df['price'] > 0)
    df.loc[mask, 'price_per_sqm'] = df.loc[mask, 'price'] / df.loc[mask, 'area']
    
    # Заменяем оставшиеся NaN в price_per_sqm на 0
    df['price_per_sqm'] = df['price_per_sqm'].fillna(0)
    
    # Сохраняем исправленный файл
    fixed_file = 'comprehensive_real_estate_dataset_fixed.csv'
    df.to_csv(fixed_file, index=False, encoding='utf-8')
    print(f"\nИсправленные данные сохранены в {fixed_file}")
    
    return fixed_file

if __name__ == "__main__":
    print("=" * 50)
    print("ЗАГРУЗКА ДАННЫХ НЕДВИЖИМОСТИ В БАЗУ ДАННЫХ")
    print("=" * 50)
    
    # Проверяем подключение
    if not check_db_connection():
        print("Не удалось подключиться к БД. Проверьте настройки.")
        exit(1)
    
    # Исправляем данные в CSV
    fixed_csv = fix_csv_data()
    
    # Загружаем данные
    try:
        load_csv_to_db(fixed_csv)
        print("\nГотово!")
    except FileNotFoundError:
        print("Ошибка: CSV файл не найден!")
        print("Сначала запустите скрипт создания датасета.")
    except Exception as e:
        print(f"Ошибка: {e}")