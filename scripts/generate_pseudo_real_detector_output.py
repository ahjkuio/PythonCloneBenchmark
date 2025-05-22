import pandas as pd
import argparse
import os
from tqdm import tqdm

def get_normalized_lines(file_path):
    """Читает файл, возвращает множество нормализованных непустых строк (без комментариев)."""
    lines = set()
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                stripped_line = line.strip()
                if stripped_line and not stripped_line.startswith('#'):
                    lines.add(stripped_line)
    except FileNotFoundError:
        print(f"Предупреждение: Файл не найден {file_path}")
        return None
    return lines

def main():
    parser = argparse.ArgumentParser(description="Генерирует псевдо-реальный CSV файл результатов детектора на основе процента совпадения строк.")
    parser.add_argument('--benchmark_csv', required=True, help="Путь к эталонному CSV файлу с парами (например, clones_ГОД.csv).")
    parser.add_argument('--output_csv', required=True, help="Путь для сохранения CSV файла с результатами псевдо-детектора.")
    parser.add_argument('--threshold', type=float, default=0.7, help="Порог совпадения строк для признания пары клоном (0.0-1.0).")
    parser.add_argument('--year', type=str, required=True, help="Год обрабатываемых данных (например, 2017), для корректного формирования путей.")


    args = parser.parse_args()

    print(f"Чтение эталонного CSV: {args.benchmark_csv}")
    try:
        benchmark_df = pd.read_csv(args.benchmark_csv)
    except FileNotFoundError:
        print(f"Ошибка: Эталонный CSV файл не найден: {args.benchmark_csv}")
        return

    detected_clones_data = []
    
    # Для корректного подсчета строк в оригинальных файлах, которые были извлечены build_benchmark.py
    # Нам нужно знать, где лежат extracted_solutions
    # Предполагаем, что этот скрипт запускается из директории scripts/
    # и extracted_solutions находится на уровень выше.
    base_path_to_solutions = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


    print(f"Обработка пар с порогом {args.threshold}...")
    for index, row in tqdm(benchmark_df.iterrows(), total=benchmark_df.shape[0], desc="Генерация псевдо-клонов"):
        file1_relative_path = row['file1_path']
        file2_relative_path = row['file2_path']

        # Формируем абсолютные пути, если они относительные в CSV
        # Пути в clones_ГОД.csv должны быть относительными от корня проекта PythonCloneBenchmark
        file1_abs_path = os.path.join(base_path_to_solutions, file1_relative_path)
        file2_abs_path = os.path.join(base_path_to_solutions, file2_relative_path)

        lines1 = get_normalized_lines(file1_abs_path)
        lines2 = get_normalized_lines(file2_abs_path)

        if lines1 is None or lines2 is None:
            # Пропускаем пару, если один из файлов не найден
            continue
            
        if not lines1 or not lines2: # если один из файлов пуст (после нормализации)
            # print(f"Пропуск пары из-за пустого файла (после нормализации): {file1_abs_path} или {file2_abs_path}")
            continue


        intersection_count = len(lines1.intersection(lines2))
        min_len = min(len(lines1), len(lines2))
        
        if min_len == 0: # Чтобы избежать деления на ноль, если оба файла пусты (хотя уже проверено выше)
            similarity = 0
        else:
            similarity = intersection_count / min_len

        if similarity >= args.threshold:
            # Получаем количество строк для fileX_end (оригинальных, до нормализации)
            # Используем пути, которые были в CSV, так как они соответствуют структуре, созданной build_benchmark.py
            
            try:
                with open(file1_abs_path, 'r', encoding='utf-8', errors='ignore') as f1:
                    num_lines_f1 = sum(1 for _ in f1)
                with open(file2_abs_path, 'r', encoding='utf-8', errors='ignore') as f2:
                    num_lines_f2 = sum(1 for _ in f2)
            except FileNotFoundError:
                # Это уже должно быть обработано get_normalized_lines, но на всякий случай
                print(f"Предупреждение: Один из файлов не найден при подсчете строк: {file1_abs_path} или {file2_abs_path}")
                continue

            detected_clones_data.append({
                'file1_path': file1_relative_path, # Сохраняем относительные пути, как в эталоне
                'file1_start': 0,
                'file1_end': num_lines_f1 - 1 if num_lines_f1 > 0 else 0,
                'file2_path': file2_relative_path, # Сохраняем относительные пути, как в эталоне
                'file2_start': 0,
                'file2_end': num_lines_f2 - 1 if num_lines_f2 > 0 else 0
            })

    output_df = pd.DataFrame(detected_clones_data)
    
    # Создаем директорию для output_csv, если она не существует
    output_dir = os.path.dirname(args.output_csv)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Создана директория: {output_dir}")

    print(f"Сохранение {len(output_df)} обнаруженных псевдо-клонов в: {args.output_csv}")
    output_df.to_csv(args.output_csv, index=False)
    print("Готово.")

if __name__ == '__main__':
    main() 