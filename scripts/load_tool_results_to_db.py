import sqlite3
import pandas as pd
import argparse
import os

def main():
    parser = argparse.ArgumentParser(description="Загрузка результатов работы детектора клонов из CSV в базу данных SQLite.")
    parser.add_argument("--csv_file", type=str, required=True, help="Путь к CSV файлу с результатами детектора.")
    parser.add_argument("--db_file", type=str, required=True, help="Путь к файлу базы данных SQLite.")
    
    args = parser.parse_args()

    if not os.path.exists(args.csv_file):
        print(f"Ошибка: CSV файл не найден: {args.csv_file}")
        return

    # Создаем директорию для БД, если ее нет
    db_dir = os.path.dirname(args.db_file)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)
        print(f"Создана директория для БД: {db_dir}")

    try:
        # Читаем CSV файл
        # Используем engine='python' для более гибкой обработки кавычек и разделителей, если стандартный не справляется
        # Однако, для простых CSV pandas обычно справляется автоматически.
        # Указываем dtype=str, чтобы pandas не пытался угадывать типы и не конвертировал пути или числовые ID в числа там, где не нужно.
        # Позже мы преобразуем start/end в int.
        df = pd.read_csv(args.csv_file, dtype=str) 
        print(f"Успешно прочитан CSV файл: {args.csv_file}")
        print(f"Заголовки в CSV: {df.columns.tolist()}")
        print(f"Прочитано строк (без заголовка): {len(df)}")

        # Проверяем наличие необходимых колонок
        required_columns = ['file1_path', 'file1_start', 'file1_end', 'file2_path', 'file2_start', 'file2_end']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Ошибка: В CSV файле отсутствуют необходимые колонки: {', '.join(missing_columns)}")
            return
            
        # Преобразуем колонки с координатами в числовые, обрабатывая возможные ошибки
        for col in ['file1_start', 'file1_end', 'file2_start', 'file2_end']:
            df[col] = pd.to_numeric(df[col], errors='coerce') # errors='coerce' заменит нечисловые значения на NaN
        
        # Удаляем строки, где координаты не удалось преобразовать в числа
        original_rows = len(df)
        df.dropna(subset=['file1_start', 'file1_end', 'file2_start', 'file2_end'], inplace=True)
        if len(df) < original_rows:
            print(f"Предупреждение: {original_rows - len(df)} строк были удалены из-за некорректных (нечисловых) значений в колонках координат.")

        # Преобразуем в int после удаления NaN
        for col in ['file1_start', 'file1_end', 'file2_start', 'file2_end']:
            df[col] = df[col].astype(int)

    except Exception as e:
        print(f"Ошибка при чтении или обработке CSV файла {args.csv_file}: {e}")
        return

    if df.empty and original_rows > 0 : # Если все строки были отфильтрованы из-за ошибок
        print("Нет данных для загрузки в БД после обработки ошибок в координатах.")
        return
    elif df.empty:
        print("CSV файл пуст или не содержит корректных данных. В БД ничего не будет загружено.")
        # Не завершаем с ошибкой, просто информируем. Таблица будет создана/очищена.
        

    conn = None
    try:
        conn = sqlite3.connect(args.db_file)
        cursor = conn.cursor()
        
        table_name = "detected_clones"

        # Удаляем таблицу, если она существует, для полной перезаписи
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f"Таблица {table_name} удалена (если существовала).")

        # Создаем таблицу
        cursor.execute(f"""
        CREATE TABLE {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file1_path TEXT NOT NULL,
            file1_start INTEGER NOT NULL,
            file1_end INTEGER NOT NULL,
            file2_path TEXT NOT NULL,
            file2_start INTEGER NOT NULL,
            file2_end INTEGER NOT NULL
        )
        """)
        print(f"Таблица {table_name} успешно создана.")

        # Загружаем данные из DataFrame в SQLite таблицу
        # Убираем индекс pandas из CSV при загрузке, так как у нас есть свой AUTOINCREMENT id
        if not df.empty:
            df.to_sql(table_name, conn, if_exists='append', index=False)
            print(f"{len(df)} строк успешно загружено в таблицу {table_name}.")
        else:
            print(f"Нет данных для загрузки в таблицу {table_name}.")

        conn.commit()
        
    except sqlite3.Error as e:
        print(f"Ошибка SQLite: {e}")
    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")
    finally:
        if conn:
            conn.close()
            print(f"Соединение с БД {args.db_file} закрыто.")

if __name__ == "__main__":
    main() 