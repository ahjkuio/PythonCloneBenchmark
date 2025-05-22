import pandas as pd
import argparse
import os
import csv
from tqdm import tqdm

# Определение языка по расширению файла (упрощенно)
LANGUAGE_EXTENSIONS = {
    '.py': 'Python',
    # Можно добавить другие языки и расширения при необходимости
}

# Директория для распакованных CSV файлов (относительно корня проекта)
GCJ_UNPACKED_ROOT_SUBDIR = "data/gcj_csv_unpacked"

def get_language_from_filename(filename):
    _, ext = os.path.splitext(filename)
    return LANGUAGE_EXTENSIONS.get(ext.lower())

def main():
    parser = argparse.ArgumentParser(description="Скрипт для сборки бенчмарка Python-клонов из данных Google Code Jam.")
    parser.add_argument("--year", required=True, help="Год для обработки (например, 2017).")
    parser.add_argument("--input_csv_path", help=(
        "Путь к CSV-файлу Google Code Jam для указанного года. "
        "Если не указан, будет сформирован стандартный путь вида: data/gcj_csv_unpacked/gcjГОД.csv "
        "относительно корня проекта."
    ))
    parser.add_argument("--extracted_solutions_dir", default="extracted_solutions", help=(
        "Директория для сохранения извлеченных .py файлов (например, 'extracted_solutions'). "
        "Будет создана относительно корня проекта, если не существует."
    ))
    parser.add_argument("--benchmark_output_dir", default="benchmark_output", help=(
        "Директория для сохранения итоговых CSV файлов с парами клонов (например, 'benchmark_output'). "
        "Будет создана относительно корня проекта, если не существует."
    ))
    
    args = parser.parse_args()

    # Определяем корень проекта (директория, содержащая директорию scripts)
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(scripts_dir, '..'))

    # Определяем путь к входному CSV файлу
    if args.input_csv_path:
        # Если пользователь указал путь, считаем его от текущей рабочей директории, если он относительный
        actual_input_csv = os.path.abspath(args.input_csv_path)
    else:
        # Если путь не указан, формируем стандартный путь от корня проекта
        actual_input_csv = os.path.join(project_root, GCJ_UNPACKED_ROOT_SUBDIR, f"gcj{args.year}.csv")

    if not os.path.exists(actual_input_csv):
        print(f"Ошибка: Входной CSV файл не найден: {actual_input_csv}")
        print(f"Пожалуйста, убедитесь, что файл существует, или запустите ")
        print(f"  python scripts/setup_project.py --year {args.year}")
        print("для его скачивания и подготовки необходимых директорий.")
        return

    # Формируем абсолютные пути для выходных директорий от корня проекта
    extracted_solutions_base_dir = os.path.join(project_root, args.extracted_solutions_dir)
    extracted_solutions_year_dir = os.path.join(extracted_solutions_base_dir, args.year)
    benchmark_output_abs_dir = os.path.join(project_root, args.benchmark_output_dir)
    
    # Создаем выходные директории (setup_project.py должен был их создать, но для надежности)
    os.makedirs(extracted_solutions_year_dir, exist_ok=True)
    os.makedirs(benchmark_output_abs_dir, exist_ok=True)

    # Путь к итоговому файлу с парами клонов
    output_clones_csv = os.path.join(benchmark_output_abs_dir, f"clones_{args.year}.csv")

    solutions_data = [] # Для хранения информации о извлеченных решениях
    python_solutions_by_task = {} # Для группировки Python-решений по задачам

    print(f"Чтение и обработка файла: {actual_input_csv}")
    try:
        with open(actual_input_csv, 'r', encoding='utf-8', errors='ignore') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in tqdm(reader, desc=f"Обработка {args.year}"):
                year_from_row = row.get('year')
                task_id = row.get('task') 
                username = row.get('username')
                solution_filename = row.get('file') 
                source_code = row.get('flines') 
                # full_path_original = row.get('full_path') # Пока не используется напрямую

                if not all([year_from_row, task_id, username, solution_filename, source_code]):
                    continue # Пропуск строки, если не хватает ключевых данных
                
                if year_from_row != args.year:
                    continue # Обрабатываем только решения для указанного года
                
                language = get_language_from_filename(solution_filename)
                if language == 'Python':
                    # Структура директорий: extracted_solutions_dir/ГОД/TASK_ID/USERNAME/
                    user_solution_dir = os.path.join(extracted_solutions_year_dir, task_id, username)
                    os.makedirs(user_solution_dir, exist_ok=True)
                    
                    safe_solution_filename = solution_filename.replace('/', '_').replace('\\', '_')
                    solution_file_path_abs = os.path.join(user_solution_dir, safe_solution_filename)
                    
                    # Относительный путь от корня проекта для хранения в CSV
                    relative_solution_file_path = os.path.relpath(solution_file_path_abs, project_root)

                    try:
                        with open(solution_file_path_abs, 'w', encoding='utf-8') as f_out:
                            f_out.write(source_code)
                        
                        num_lines = source_code.count('\n') + 1
                        
                        solutions_data.append({
                            # 'original_full_path': full_path_original,
                            'year': year_from_row,
                            'task_id': task_id,
                            'username': username,
                            'solution_filename': safe_solution_filename,
                            'saved_file_path': relative_solution_file_path, 
                            'num_lines': num_lines
                        })
                        
                        if task_id not in python_solutions_by_task:
                            python_solutions_by_task[task_id] = []
                        python_solutions_by_task[task_id].append({
                            'path': relative_solution_file_path, 
                            'lines': num_lines
                        })

                    except IOError as e:
                        print(f"Ошибка записи файла {solution_file_path_abs}: {e}")
                    except Exception as e:
                        print(f"Непредвиденная ошибка при обработке решения {solution_file_path_abs}: {e}")
                        
    except FileNotFoundError:
        # Эта ошибка уже должна быть перехвачена ранее, но для полноты
        print(f"Критическая ошибка: Файл {actual_input_csv} не найден после проверки. Это не должно было произойти.")
        return
    except Exception as e:
        print(f"Ошибка при чтении или обработке CSV файла {actual_input_csv}: {e}")
        return

    clone_pairs = []
    print("Генерация пар клонов...")
    for task_id, solutions in tqdm(python_solutions_by_task.items(), desc="Генерация пар"):
        if len(solutions) > 1: 
            for i in range(len(solutions)):
                for j in range(i + 1, len(solutions)):
                    s1 = solutions[i]
                    s2 = solutions[j]
                    clone_pairs.append({
                        'file1_path': s1['path'],
                        'file1_start': 0,
                        'file1_end': s1['lines'] - 1 if s1['lines'] > 0 else 0,
                        'file2_path': s2['path'],
                        'file2_start': 0,
                        'file2_end': s2['lines'] - 1 if s2['lines'] > 0 else 0,
                        'task_id': task_id
                    })
    
    clones_df = pd.DataFrame(clone_pairs)
    try:
        clones_df.to_csv(output_clones_csv, index=False)
        print(f"Бенчмарк для года {args.year} успешно создан: {output_clones_csv}")
        print(f"Всего извлечено Python решений: {len(solutions_data)}")
        print(f"Всего сгенерировано пар клонов: {len(clones_df)}")
    except Exception as e:
        print(f"Ошибка при сохранении CSV файла с парами клонов {output_clones_csv}: {e}")

if __name__ == '__main__':
    main() 