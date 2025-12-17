-- ============================================
-- СКРИПТ ВОССТАНОВЛЕНИЯ БАЗЫ ДАННЫХ
-- Хранилище данных недвижимости Москвы
-- Дата создания: 30.11.2024
-- ============================================

-- Отключаем проверку внешних ключей для чистого восстановления
SET session_replication_role = 'replica';

-- 1. УДАЛЕНИЕ СУЩЕСТВУЮЩИХ ТАБЛИЦ (если есть)
DROP TABLE IF EXISTS fact_real_estate CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;
DROP TABLE IF EXISTS dim_commercial_purpose CASCADE;
DROP TABLE IF EXISTS dim_house_types CASCADE;
DROP TABLE IF EXISTS dim_property_types CASCADE;
DROP TABLE IF EXISTS dim_districts CASCADE;

-- 2. СОЗДАНИЕ ТАБЛИЦ ИЗМЕРЕНИЙ (DIMENSION TABLES)

-- 2.1. Районы Москвы
CREATE TABLE dim_districts (
    district_id SERIAL PRIMARY KEY,
    district_name VARCHAR(50) NOT NULL UNIQUE,
    district_type VARCHAR(10) NOT NULL CHECK (district_type IN ('ЦАО', 'САО', 'СВАО', 'ВАО', 'ЮВАО', 'ЮАО', 'ЮЗАО', 'ЗАО', 'СЗАО', 'НАО')),
    avg_price_per_sqm DECIMAL(12,2),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2.2. Типы недвижимости
CREATE TABLE dim_property_types (
    property_type_id SERIAL PRIMARY KEY,
    property_category VARCHAR(20) NOT NULL CHECK (property_category IN ('жилая', 'коммерческая', 'земля', 'прочая')),
    property_type_name VARCHAR(50) NOT NULL UNIQUE,
    description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2.3. Типы домов
CREATE TABLE dim_house_types (
    house_type_id SERIAL PRIMARY KEY,
    house_type_name VARCHAR(50) NOT NULL UNIQUE,
    construction_material VARCHAR(30),
    typical_series VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2.4. Назначение коммерческой недвижимости
CREATE TABLE dim_commercial_purpose (
    purpose_id SERIAL PRIMARY KEY,
    purpose_name VARCHAR(50) NOT NULL UNIQUE,
    commercial_category VARCHAR(30) NOT NULL CHECK (commercial_category IN ('офис', 'торговля', 'склад', 'производство', 'готовый бизнес')),
    description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2.5. Время (date dimension)
CREATE TABLE dim_time (
    date_id SERIAL PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day INTEGER NOT NULL CHECK (day BETWEEN 1 AND 31),
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    day_of_week VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. СОЗДАНИЕ ТАБЛИЦЫ ФАКТОВ (FACT TABLE)

CREATE TABLE fact_real_estate (
    fact_id BIGSERIAL PRIMARY KEY,
    district_id INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    property_type_id INTEGER NOT NULL,
    house_type_id INTEGER,
    commercial_purpose_id INTEGER,
    
    -- Основные метрики
    price DECIMAL(15,2) NOT NULL,
    area DECIMAL(10,2) NOT NULL,
    price_per_sqm DECIMAL(12,2) NOT NULL,
    
    -- Параметры для жилой недвижимости
    rooms INTEGER,
    floor INTEGER,
    total_floors INTEGER,
    year_built INTEGER,
    
    -- Параметры для коммерческой недвижимости
    ceiling_height DECIMAL(4,2),
    has_ventilation BOOLEAN DEFAULT FALSE,
    has_air_conditioning BOOLEAN DEFAULT FALSE,
    parking_spaces INTEGER,
    land_area DECIMAL(10,2),
    
    -- Общие параметры
    metro_time INTEGER,
    has_elevator BOOLEAN DEFAULT FALSE,
    is_renovated BOOLEAN DEFAULT FALSE,
    
    -- Технические поля
    data_source VARCHAR(20) DEFAULT 'avito',
    external_id VARCHAR(100),
    address TEXT,
    url TEXT,
    created_date DATE NOT NULL,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Внешние ключи
    CONSTRAINT fk_district FOREIGN KEY (district_id) REFERENCES dim_districts(district_id),
    CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES dim_time(date_id),
    CONSTRAINT fk_property_type FOREIGN KEY (property_type_id) REFERENCES dim_property_types(property_type_id),
    CONSTRAINT fk_house_type FOREIGN KEY (house_type_id) REFERENCES dim_house_types(house_type_id),
    CONSTRAINT fk_commercial_purpose FOREIGN KEY (commercial_purpose_id) REFERENCES dim_commercial_purpose(purpose_id)
);

-- 4. СОЗДАНИЕ ИНДЕКСОВ

CREATE INDEX idx_fact_district ON fact_real_estate(district_id);
CREATE INDEX idx_fact_price ON fact_real_estate(price);
CREATE INDEX idx_fact_rooms ON fact_real_estate(rooms);
CREATE INDEX idx_fact_date ON fact_real_estate(created_date);
CREATE INDEX idx_fact_property_type ON fact_real_estate(property_type_id);
CREATE INDEX idx_fact_price_sqm ON fact_real_estate(price_per_sqm);
CREATE INDEX idx_fact_rooms_district ON fact_real_estate(rooms, district_id);
CREATE INDEX idx_fact_date_category ON fact_real_estate(created_date, property_type_id);

-- Включаем проверку внешних ключей
SET session_replication_role = 'origin';

-- 5. НАПОЛНЕНИЕ СПРАВОЧНИКОВ

-- 5.1. Районы Москвы
INSERT INTO dim_districts (district_name, district_type) VALUES
('ЦАО', 'ЦАО'),
('ЗАО', 'ЗАО'),
('СЗАО', 'СЗАО'),
('САО', 'САО'),
('СВАО', 'СВАО'),
('ВАО', 'ВАО'),
('ЮВАО', 'ЮВАО'),
('ЮАО', 'ЮАО'),
('ЮЗАО', 'ЮЗАО'),
('НАО', 'НАО');

-- 5.2. Типы недвижимости
INSERT INTO dim_property_types (property_category, property_type_name, description) VALUES
('жилая', 'квартира', 'Квартиры в многоквартирных домах'),
('жилая', 'апартаменты', 'Апартаменты с правом проживания'),
('жилая', 'комната', 'Отдельные комнаты в квартирах'),
('жилая', 'дом', 'Частные дома и коттеджи'),
('жилая', 'таунхаус', 'Блокированные дома'),
('коммерческая', 'офис', 'Офисные помещения'),
('коммерческая', 'торговое помещение', 'Торговые площади'),
('коммерческая', 'склад', 'Складские помещения'),
('коммерческая', 'производственное помещение', 'Производственные площади'),
('коммерческая', 'готовый бизнес', 'Готовые бизнес-объекты'),
('земля', 'участок', 'Земельные участки'),
('прочая', 'гараж', 'Гаражи и машиноместа'),
('прочая', 'машиноместо', 'Машиноместа');

-- 5.3. Типы домов
INSERT INTO dim_house_types (house_type_name, construction_material, typical_series) VALUES
('панельный', 'панель', 'П-44, П-3, П-46'),
('кирпичный', 'кирпич', 'сталинка, хрущевка'),
('монолитный', 'монолит', 'современный'),
('блочный', 'блоки', 'II-68, II-49'),
('коммерческий', 'композит', 'бизнес-центр'),
('гаражный комплекс', 'металл', 'типовой');

-- 5.4. Назначение коммерческой недвижимости
INSERT INTO dim_commercial_purpose (purpose_name, commercial_category, description) VALUES
('бизнес-центр', 'офис', 'Офисы в бизнес-центрах'),
('отдельное здание', 'офис', 'Отдельные офисные здания'),
('торговый центр', 'торговля', 'Помещения в торговых центрах'),
('отдельный магазин', 'торговля', 'Отдельные торговые объекты'),
('логистический комплекс', 'склад', 'Склады в логистических парках'),
('производственный цех', 'производство', 'Производственные помещения'),
('готовый бизнес', 'готовый бизнес', 'Готовые работающие бизнесы');

-- 5.5. Таблица времени (последние 90 дней)
INSERT INTO dim_time (full_date, day, month, year, quarter, day_of_week, is_weekend)
SELECT 
    date::date as full_date,
    EXTRACT(DAY FROM date) as day,
    EXTRACT(MONTH FROM date) as month,
    EXTRACT(YEAR FROM date) as year,
    EXTRACT(QUARTER FROM date) as quarter,
    TO_CHAR(date, 'Day') as day_of_week,
    EXTRACT(ISODOW FROM date) IN (6,7) as is_weekend
FROM generate_series(
    CURRENT_DATE - INTERVAL '90 days', 
    CURRENT_DATE, 
    '1 day'::interval
) as date;

-- 6. СОЗДАНИЕ ПРЕДСТАВЛЕНИЙ ДЛЯ АНАЛИЗА

-- 6.1. Обзор рынка по районам
CREATE OR REPLACE VIEW vw_market_overview AS
SELECT 
    d.district_name,
    d.district_type,
    COUNT(*) as total_offers,
    AVG(f.price) as avg_price,
    AVG(f.price_per_sqm) as avg_price_sqm,
    MIN(f.price_per_sqm) as min_price_sqm,
    MAX(f.price_per_sqm) as max_price_sqm,
    COUNT(DISTINCT f.property_type_id) as property_types_available
FROM fact_real_estate f
JOIN dim_districts d ON f.district_id = d.district_id
GROUP BY d.district_name, d.district_type
ORDER BY avg_price_sqm DESC;

-- 6.2. Анализ жилой недвижимости
CREATE OR REPLACE VIEW vw_residential_analysis AS
SELECT 
    d.district_name,
    pt.property_type_name,
    f.rooms,
    COUNT(*) as offers_count,
    AVG(f.price) as avg_price,
    AVG(f.price_per_sqm) as avg_price_sqm,
    AVG(f.area) as avg_area,
    AVG(f.metro_time) as avg_metro_time
FROM fact_real_estate f
JOIN dim_districts d ON f.district_id = d.district_id
JOIN dim_property_types pt ON f.property_type_id = pt.property_type_id
WHERE pt.property_category = 'жилая'
GROUP BY d.district_name, pt.property_type_name, f.rooms
ORDER BY d.district_name, pt.property_type_name, f.rooms;

-- 6.3. Анализ коммерческой недвижимости
CREATE OR REPLACE VIEW vw_commercial_analysis AS
SELECT 
    d.district_name,
    cp.purpose_name,
    cp.commercial_category,
    COUNT(*) as offers_count,
    AVG(f.price) as avg_price,
    AVG(f.price_per_sqm) as avg_price_sqm,
    AVG(f.area) as avg_area
FROM fact_real_estate f
JOIN dim_districts d ON f.district_id = d.district_id
JOIN dim_commercial_purpose cp ON f.commercial_purpose_id = cp.purpose_id
GROUP BY d.district_name, cp.purpose_name, cp.commercial_category
ORDER BY avg_price_sqm DESC;

-- 7. ФУНКЦИЯ ДЛЯ ПРОВЕРКИ ЦЕЛОСТНОСТИ ДАННЫХ

CREATE OR REPLACE FUNCTION check_data_integrity()
RETURNS TABLE (
    check_name VARCHAR(100),
    status VARCHAR(20),
    details TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 'Всего записей в fact_real_estate'::VARCHAR, 
           'OK'::VARCHAR, 
           COUNT(*)::TEXT FROM fact_real_estate
    UNION ALL
    SELECT 'Записей без district_id', 
           CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'ERROR' END,
           COUNT(*)::TEXT 
    FROM fact_real_estate WHERE district_id IS NULL
    UNION ALL
    SELECT 'Записей без property_type_id', 
           CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'ERROR' END,
           COUNT(*)::TEXT 
    FROM fact_real_estate WHERE property_type_id IS NULL
    UNION ALL
    SELECT 'Записей с нулевой ценой', 
           CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'WARNING' END,
           COUNT(*)::TEXT 
    FROM fact_real_estate WHERE price <= 0
    UNION ALL
    SELECT 'Уникальных external_id', 
           'INFO',
           COUNT(DISTINCT external_id)::TEXT 
    FROM fact_real_estate;
END;
$$ LANGUAGE plpgsql;

-- 8. КОММЕНТАРИИ К ТАБЛИЦАМ (документация)

COMMENT ON TABLE dim_districts IS 'Справочник административных районов Москвы';
COMMENT ON TABLE dim_property_types IS 'Классификация типов недвижимости';
COMMENT ON TABLE dim_house_types IS 'Типы домов по материалу строительства';
COMMENT ON TABLE dim_commercial_purpose IS 'Назначение коммерческой недвижимости';
COMMENT ON TABLE dim_time IS 'Временная dimension таблица';
COMMENT ON TABLE fact_real_estate IS 'Основная таблица фактов с данными о недвижимости';

COMMENT ON COLUMN fact_real_estate.price IS 'Цена объекта в рублях';
COMMENT ON COLUMN fact_real_estate.area IS 'Площадь объекта в м²';
COMMENT ON COLUMN fact_real_estate.price_per_sqm IS 'Цена за квадратный метр в рублях';
COMMENT ON COLUMN fact_real_estate.rooms IS 'Количество комнат (0 для студий)';
COMMENT ON COLUMN fact_real_estate.metro_time IS 'Время до метро в минутах';

-- 9. ИНФОРМАЦИОННОЕ СООБЩЕНИЕ

DO $$
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'БАЗА ДАННЫХ "real_estate_moscow" УСПЕШНО СОЗДАНА';
    RAISE NOTICE 'Дата создания: %', CURRENT_TIMESTAMP;
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Создано таблиц: 6';
    RAISE NOTICE 'Создано представлений: 3';
    RAISE NOTICE 'Создано индексов: 8';
    RAISE NOTICE '============================================';
END $$;

-- 10. ВЫВОД СТАТИСТИКИ ПОСЛЕ ВОССТАНОВЛЕНИЯ

SELECT 
    (SELECT COUNT(*) FROM dim_districts) as districts_count,
    (SELECT COUNT(*) FROM dim_property_types) as property_types_count,
    (SELECT COUNT(*) FROM dim_house_types) as house_types_count,
    (SELECT COUNT(*) FROM dim_commercial_purpose) as commercial_purposes_count,
    (SELECT COUNT(*) FROM dim_time) as time_records_count,
    (SELECT COUNT(*) FROM fact_real_estate) as fact_records_count;