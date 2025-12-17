import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def create_comprehensive_real_estate_dataset():
    """Создание комплексного датасета всех видов недвижимости Москвы"""
    
    np.random.seed(42)
    n_samples = 3000  # Увеличили объем данных
    
    # Районы Москвы с разной ценовой категорией
    districts = {
        'ЦАО': {'price_multiplier': 1.8, 'count': 400},
        'ЗАО': {'price_multiplier': 1.4, 'count': 350},
        'СЗАО': {'price_multiplier': 1.2, 'count': 280},
        'САО': {'price_multiplier': 1.1, 'count': 320},
        'СВАО': {'price_multiplier': 1.0, 'count': 300},
        'ВАО': {'price_multiplier': 0.9, 'count': 290},
        'ЮВАО': {'price_multiplier': 0.85, 'count': 310},
        'ЮАО': {'price_multiplier': 0.9, 'count': 280},
        'ЮЗАО': {'price_multiplier': 1.1, 'count': 320},
        'НАО': {'price_multiplier': 0.7, 'count': 250}
    }
    
    # Категории недвижимости с их параметрами
    property_categories = {
        'жилая': {
            'квартира': {'base_price_sqm': 150000, 'count': 1200, 'area_range': (30, 120)},
            'апартаменты': {'base_price_sqm': 180000, 'count': 300, 'area_range': (25, 80)},
            'комната': {'base_price_sqm': 120000, 'count': 200, 'area_range': (10, 25)},
            'дом': {'base_price_sqm': 200000, 'count': 150, 'area_range': (80, 300)},
            'таунхаус': {'base_price_sqm': 170000, 'count': 100, 'area_range': (60, 150)}
        },
        'коммерческая': {
            'офис': {'base_price_sqm': 80000, 'count': 200, 'area_range': (50, 500)},
            'торговое помещение': {'base_price_sqm': 120000, 'count': 150, 'area_range': (30, 300)},
            'склад': {'base_price_sqm': 40000, 'count': 100, 'area_range': (100, 2000)},
            'производственное помещение': {'base_price_sqm': 50000, 'count': 80, 'area_range': (200, 1500)},
            'готовый бизнес': {'base_price_sqm': 0, 'count': 70, 'area_range': (0, 0)}  # цена за бизнес
        },
        'земля': {
            'участок': {'base_price_sqm': 5000, 'count': 100, 'area_range': (100, 1500)}
        },
        'прочая': {
            'гараж': {'base_price_sqm': 0, 'count': 80, 'area_range': (15, 30)},
            'машиноместо': {'base_price_sqm': 0, 'count': 70, 'area_range': (0, 0)}
        }
    }
    
    data = []
    id_counter = 1
    
    for district, district_params in districts.items():
        district_count = 0
        
        for category, types in property_categories.items():
            for prop_type, type_params in types.items():
                # Распределяем количество объявлений по типам пропорционально
                type_count = max(1, type_params['count'] * district_params['count'] // n_samples)
                
                for i in range(type_count):
                    if district_count >= district_params['count']:
                        break
                        
                    # Генерация данных в зависимости от категории
                    property_data = generate_property_data(
                        category, prop_type, type_params, district, district_params
                    )
                    
                    if property_data:
                        property_data['id'] = f"avito_{id_counter:06d}"
                        property_data['property_category'] = category
                        property_data['property_type'] = prop_type
                        data.append(property_data)
                        id_counter += 1
                        district_count += 1
    
    df = pd.DataFrame(data)
    return df

def generate_property_data(category, prop_type, type_params, district, district_params):
    """Генерация данных для конкретного типа недвижимости"""
    
    base_data = {
        'district': district,
        'publish_date': (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d')
    }
    
    if category == 'жилая':
        return generate_residential_data(base_data, prop_type, type_params, district_params)
    elif category == 'коммерческая':
        return generate_commercial_data(base_data, prop_type, type_params, district_params)
    elif category == 'земля':
        return generate_land_data(base_data, prop_type, type_params, district_params)
    elif category == 'прочая':
        return generate_other_data(base_data, prop_type, type_params, district_params)

def generate_residential_data(base_data, prop_type, type_params, district_params):
    """Генерация данных для жилой недвижимости"""
    
    area_min, area_max = type_params['area_range']
    area = max(area_min, np.random.normal((area_min + area_max) / 2, (area_max - area_min) / 6))
    
    if prop_type == 'комната':
        rooms = 0  # комната обозначается как 0 комнат
        base_price = area * type_params['base_price_sqm']
    elif prop_type == 'дом' or prop_type == 'таунхаус':
        rooms = random.choice([3, 4, 5, 6])
        base_price = area * type_params['base_price_sqm']
    else:  # квартира, апартаменты
        rooms = random.choice([1, 2, 3, 4])
        base_price = area * type_params['base_price_sqm']
    
    price = int(base_price * district_params['price_multiplier'] * 
               (1 + (rooms - 1) * 0.15) * np.random.uniform(0.8, 1.2))
    
    floor = random.randint(1, 25) if prop_type in ['квартира', 'апартаменты', 'комната'] else 1
    total_floors = max(floor, random.randint(5, 25)) if prop_type in ['квартира', 'апартаменты', 'комната'] else random.randint(1, 3)
    
    return {
        **base_data,
        'rooms': rooms,
        'area': round(area, 1),
        'price': price,
        'price_per_sqm': int(price / area) if area > 0 else 0,
        'floor': floor,
        'total_floors': total_floors,
        'year_built': random.randint(1960, 2023),
        'house_type': random.choice(['панельный', 'кирпичный', 'монолитный', 'блочный']),
        'metro_time': random.randint(5, 30),
        'address': f"г. Москва, {base_data['district']}, ул. Примерная, д. {random.randint(1, 100)}",
        'url': generate_url(prop_type, rooms, area, floor, total_floors),
        'ceiling_height': round(np.random.normal(2.7, 0.2), 1) if prop_type != 'комната' else None,
        'has_elevator': random.choice([True, False]) if total_floors > 5 else True,
        'is_renovated': random.choice([True, False])
    }

def generate_commercial_data(base_data, prop_type, type_params, district_params):
    """Генерация данных для коммерческой недвижимости"""
    
    area_min, area_max = type_params['area_range']
    area = max(area_min, np.random.normal((area_min + area_max) / 2, (area_max - area_min) / 6))
    
    if prop_type == 'готовый бизнес':
        # Для готового бизнеса цена не привязана к площади
        base_price = np.random.normal(5000000, 2000000)
        price_per_sqm = 0
    else:
        base_price = area * type_params['base_price_sqm']
        price_per_sqm = int(base_price / area) if area > 0 else 0
    
    price = int(base_price * district_params['price_multiplier'] * np.random.uniform(0.7, 1.3))
    
    return {
        **base_data,
        'rooms': None,
        'area': round(area, 1),
        'price': price,
        'price_per_sqm': price_per_sqm,
        'floor': random.randint(1, 10) if prop_type != 'склад' else 1,
        'total_floors': random.randint(1, 10) if prop_type != 'склад' else 1,
        'year_built': random.randint(1970, 2023),
        'house_type': 'коммерческий',
        'metro_time': random.randint(3, 25),
        'address': f"г. Москва, {base_data['district']}, ул. Коммерческая, д. {random.randint(1, 50)}",
        'url': generate_commercial_url(prop_type, area),
        'ceiling_height': round(np.random.normal(3.5, 0.5), 1) if prop_type in ['склад', 'производственное помещение'] else round(np.random.normal(2.8, 0.2), 1),
        'has_ventilation': random.choice([True, False]),
        'has_air_conditioning': random.choice([True, False]) if prop_type in ['офис', 'торговое помещение'] else False,
        'parking_spaces': random.randint(0, 20) if prop_type in ['офис', 'торговое помещение'] else 0,
        'commercial_purpose': prop_type
    }

def generate_land_data(base_data, prop_type, type_params, district_params):
    """Генерация данных для земельных участков"""
    
    area_min, area_max = type_params['area_range']
    area = max(area_min, np.random.normal((area_min + area_max) / 2, (area_max - area_min) / 6))
    
    base_price = area * type_params['base_price_sqm']
    price = int(base_price * district_params['price_multiplier'] * np.random.uniform(0.8, 1.4))
    
    return {
        **base_data,
        'rooms': None,
        'area': round(area, 1),
        'price': price,
        'price_per_sqm': int(price / area) if area > 0 else 0,
        'floor': None,
        'total_floors': None,
        'year_built': None,
        'house_type': None,
        'metro_time': random.randint(10, 45),
        'address': f"г. Москва, {base_data['district']}, земельный участок №{random.randint(1, 1000)}",
        'url': generate_land_url(area),
        'land_area': round(area, 1),
        'has_utilities': random.choice([True, False]),
        'purpose': random.choice(['ИЖС', 'коммерческое', 'сельскохозяйственное'])
    }

def generate_other_data(base_data, prop_type, type_params, district_params):
    """Генерация данных для прочей недвижимости"""
    
    if prop_type == 'гараж':
        area = random.uniform(15, 30)
        base_price = 1000000  # фиксированная базовая цена
    else:  # машиноместо
        area = 0
        base_price = 500000  # фиксированная базовая цена
    
    price = int(base_price * district_params['price_multiplier'] * np.random.uniform(0.9, 1.1))
    
    return {
        **base_data,
        'rooms': None,
        'area': round(area, 1) if area > 0 else None,
        'price': price,
        'price_per_sqm': int(price / area) if area > 0 else 0,
        'floor': random.randint(-3, 3),
        'total_floors': random.randint(1, 5),
        'year_built': random.randint(1980, 2023),
        'house_type': 'гаражный комплекс' if prop_type == 'гараж' else 'паркинг',
        'metro_time': random.randint(5, 20),
        'address': f"г. Москва, {base_data['district']}, {'гаражный кооператив' if prop_type == 'гараж' else 'паркинг'} №{random.randint(1, 50)}",
        'url': generate_other_url(prop_type),
        'has_security': random.choice([True, False]),
        'has_electricity': True
    }

def generate_url(prop_type, rooms, area, floor, total_floors):
    """Генерация URL для жилой недвижимости"""
    if prop_type == 'комната':
        return f"https://www.avito.ru/moskva/komnaty/komnata_{int(area)}_m_{floor}_{total_floors}_et.{random.randint(1000000, 9999999)}"
    elif prop_type == 'дом':
        return f"https://www.avito.ru/moskva/doma_dachi_kottedzhi/dom_{int(area)}_m.{random.randint(1000000, 9999999)}"
    elif prop_type == 'таунхаус':
        return f"https://www.avito.ru/moskva/doma_dachi_kottedzhi/taunhaus_{int(area)}_m.{random.randint(1000000, 9999999)}"
    else:
        room_text = 'kvartira' if prop_type == 'квартира' else 'apartamenty'
        rooms_text = f"{rooms}_k." if rooms > 0 else "studiya"
        return f"https://www.avito.ru/moskva/{room_text}/{rooms_text}_{int(area)}_m_{floor}_{total_floors}_et.{random.randint(1000000, 9999999)}"

def generate_commercial_url(prop_type, area):
    """Генерация URL для коммерческой недвижимости"""
    prop_type_en = {
        'офис': 'ofis',
        'торговое помещение': 'torgovoe_pomeschenie', 
        'склад': 'sklad',
        'производственное помещение': 'proizvodstvennoe_pomeschenie',
        'готовый бизнес': 'gotovyy_biznes'
    }
    return f"https://www.avito.ru/moskva/kommercheskaya_nedvizhimost/{prop_type_en[prop_type]}_{int(area)}_m.{random.randint(1000000, 9999999)}"

def generate_land_url(area):
    """Генерация URL для земельных участков"""
    return f"https://www.avito.ru/moskva/zemelnye_uchastki/uchastok_{int(area)}_sot.{random.randint(1000000, 9999999)}"

def generate_other_url(prop_type):
    """Генерация URL для прочей недвижимости"""
    if prop_type == 'гараж':
        return f"https://www.avito.ru/moskva/garazhi_i_mashinomesta/garazh.{random.randint(1000000, 9999999)}"
    else:
        return f"https://www.avito.ru/moskva/garazhi_i_mashinomesta/mashinomesto.{random.randint(1000000, 9999999)}"

def analyze_comprehensive_dataset(df):
    """Расширенный анализ комплексного датасета"""
    
    print("КОМПЛЕКСНАЯ СТАТИСТИКА ДАТАСЕТА:")
    print(f"Всего объявлений: {len(df):,}")
    print(f"Период: с {df['publish_date'].min()} по {df['publish_date'].max()}")
    
    print(f"\nРАСПРЕДЕЛЕНИЕ ПО КАТЕГОРИЯМ НЕДВИЖИМОСТИ:")
    category_stats = df['property_category'].value_counts()
    for category, count in category_stats.items():
        print(f"  • {category}: {count} объявлений ({count/len(df)*100:.1f}%)")
    
    print(f"\n💰 СТАТИСТИКА ПО ЦЕНАМ ПО КАТЕГОРИЯМ:")
    for category in df['property_category'].unique():
        category_data = df[df['property_category'] == category]
        avg_price = category_data['price'].mean()
        avg_price_sqm = category_data[category_data['price_per_sqm'] > 0]['price_per_sqm'].mean()
        print(f"  • {category}: {avg_price:,.0f} руб. | {avg_price_sqm:,.0f} руб./м²")
    
    print(f"\n🏠 ДЕТАЛИЗАЦИЯ ПО ТИПАМ НЕДВИЖИМОСТИ:")
    type_stats = df.groupby(['property_category', 'property_type']).agg({
        'price': ['count', 'mean'],
        'area': 'mean'
    }).round(0)
    
    for (category, prop_type), row in type_stats.iterrows():
        print(f"  • {category}/{prop_type}: {row[('price', 'count')]} объявлений, "
            f"ср. цена {row[('price', 'mean')]:,.0f} руб., ср. площадь {row[('area', 'mean')]:.0f} м²")

def save_comprehensive_data(df):
    """Сохранение комплексных данных для курсовой работы"""
    
    # Основной файл
    df.to_csv('comprehensive_real_estate_dataset.csv', index=False, encoding='utf-8')
    
    # Файлы по категориям
    for category in df['property_category'].unique():
        category_df = df[df['property_category'] == category]
        category_df.to_csv(f'real_estate_{category}.csv', index=False, encoding='utf-8')
    
    # Аналитические данные
    analytical_columns = ['property_category', 'property_type', 'district', 'price', 'area', 'price_per_sqm']
    df[analytical_columns].to_csv('comprehensive_analysis_data.csv', index=False)
    
    # Статистика по районам и категориям
    stats_df = df.groupby(['district', 'property_category']).agg({
        'price': ['count', 'mean', 'median'],
        'price_per_sqm': 'mean',
        'area': 'mean'
    }).round(0)
    
    stats_df.to_csv('district_category_statistics.csv')
    
    print("Файлы сохранены:")
    print("  - comprehensive_real_estate_dataset.csv (полные данные)")
    print("  - real_estate_жилая.csv, real_estate_коммерческая.csv, ... (по категориям)")
    print("  - comprehensive_analysis_data.csv (данные для анализа)")
    print("  - district_category_statistics.csv (статистика по районам и категориям)")

def main():
    print("СОЗДАНИЕ КОМПЛЕКСНОГО ДАТАСЕТА НЕДВИЖИМОСТИ")
    print("=" * 60)
    
    # Создаем комплексный датасет
    df = create_comprehensive_real_estate_dataset()
    
    # Анализируем
    analyze_comprehensive_dataset(df)
    
    # Сохраняем
    save_comprehensive_data(df)
    
    # Покажем примеры данных из разных категорий
    print("\n ПРИМЕРЫ ДАННЫХ ИЗ РАЗНЫХ КАТЕГОРИЙ:")
    for category in df['property_category'].unique():
        category_sample = df[df['property_category'] == category].head(2)
        print(f"\n{category.upper()}:")
        print(category_sample[['property_type', 'district', 'price', 'area', 'price_per_sqm']].to_string(index=False))

if __name__ == "__main__":
    main()