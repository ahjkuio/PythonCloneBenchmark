import sqlite3
import pandas as pd
import argparse
import os
import pathlib

def get_line_count(start, end):
    """Подсчитывает количество строк во фрагменте (0-индексация, включительно)."""
    if start < 0 or end < 0 or end < start: # Добавим проверку на корректность
        return 0
    return end - start + 1

def calculate_fragment_coverage(b_start, b_end, t_start, t_end):
    """
    Вычисляет двустороннее покрытие между эталонным (b) и обнаруженным (t) фрагментами.
    Возвращает: (покрытие эталона детектором, покрытие детектора эталоном)
    """
    lines_b = get_line_count(b_start, b_end)
    lines_t = get_line_count(t_start, t_end)

    # Если один из фрагментов некорректен или пуст
    if lines_b == 0 and lines_t == 0:
        return 1.0, 1.0 # Оба пустые и совпадают - полное покрытие (можно обсудить)
    if lines_b == 0 or lines_t == 0:
        return 0.0, 0.0 # Один пуст, другой нет - нет покрытия

    overlap_start = max(b_start, t_start)
    overlap_end = min(b_end, t_end)
    overlap_lines = get_line_count(overlap_start, overlap_end)

    coverage_b_by_t = overlap_lines / lines_b
    coverage_t_by_b = overlap_lines / lines_t
    
    return coverage_b_by_t, coverage_t_by_b

def calculate_c_match(benchmark_clone_row, tool_clone_data, threshold=0.7):
    """
    Проверяет c-match между эталонным клоном и клоном, обнаруженным инструментом.
    benchmark_clone_row: dict или pandas.Series с ключами 'file1_path', 'file1_start', 'file1_end', ...
    tool_clone_data: pandas.Series (из tool_df.iterrows()) или dict.
    threshold: порог покрытия.
    """
    
    # В текущей реализации из main() tool_clone_data приходит как pandas.Series.
    # pandas.Series уже поддерживает доступ по ключу (имени колонки), как словарь.
    # Поэтому дополнительное преобразование в tool_data_dict не требуется,
    # можно напрямую использовать tool_clone_data.
    # Оставляем переменную tool_data_dict для минимальных изменений ниже,
    # просто присваивая ей tool_clone_data.
    tool_data_dict = tool_clone_data

    # --- DEBUG LOGGING ---
    # print(f"  [C_MATCH_DEBUG] In calculate_c_match (tool_data type: {type(tool_clone_data)})")
    # --- END DEBUG LOGGING ---

    # Пути УЖЕ должны быть нормализованы и абсолютны к этому моменту
    b_f1_path = benchmark_clone_row['file1_path']
    b_f1_start = benchmark_clone_row['file1_start']
    b_f1_end = benchmark_clone_row['file1_end']
    b_f2_path = benchmark_clone_row['file2_path']
    b_f2_start = benchmark_clone_row['file2_start']
    b_f2_end = benchmark_clone_row['file2_end']

    t_f1_path = tool_data_dict['file1_path']
    t_f1_start = tool_data_dict['file1_start']
    t_f1_end = tool_data_dict['file1_end']
    t_f2_path = tool_data_dict['file2_path']
    t_f2_start = tool_data_dict['file2_start']
    t_f2_end = tool_data_dict['file2_end']

    # --- DEBUG LOGGING ---
    # print(f"    [C_MATCH_DEBUG] In calculate_c_match:")
    # print(f"      Benchmark paths: {b_f1_path} ({b_f1_start}-{b_f1_end}), {b_f2_path} ({b_f2_start}-{b_f2_end})")
    # print(f"      Tool paths     : {t_f1_path} ({t_f1_start}-{t_f1_end}), {t_f2_path} ({t_f2_start}-{t_f2_end})")
    # --- END DEBUG LOGGING ---

    # 1. Сопоставление файлов и фрагментов
    # Вариант 1: Прямое совпадение файлов (b1==t1, b2==t2)
    if b_f1_path == t_f1_path and b_f2_path == t_f2_path:
        # --- DEBUG LOGGING ---
        # print(f"    [C_MATCH_DEBUG] File match type: Direct (b1==t1, b2==t2)")
        # --- END DEBUG LOGGING ---
        b_frag1_coords = (b_f1_start, b_f1_end)
        t_frag1_coords = (t_f1_start, t_f1_end)
        b_frag2_coords = (b_f2_start, b_f2_end)
        t_frag2_coords = (t_f2_start, t_f2_end)
    # Вариант 2: Обратное совпадение файлов (b1==t2, b2==t1)
    elif b_f1_path == t_f2_path and b_f2_path == t_f1_path:
        # --- DEBUG LOGGING ---
        # print(f"    [C_MATCH_DEBUG] File match type: Reverse (b1==t2, b2==t1)")
        # --- END DEBUG LOGGING ---
        b_frag1_coords = (b_f1_start, b_f1_end)
        t_frag1_coords = (t_f2_start, t_f2_end) # Фрагмент 2 инструмента сопоставляется с фрагментом 1 эталона
        b_frag2_coords = (b_f2_start, b_f2_end)
        t_frag2_coords = (t_f1_start, t_f1_end) # Фрагмент 1 инструмента сопоставляется с фрагментом 2 эталона
    else:
        # --- DEBUG LOGGING ---
        # print(f"    [C_MATCH_DEBUG] File match type: None. Paths mismatch.")
        # print(f"    [C_MATCH_DEBUG] Result: False")
        # --- END DEBUG LOGGING ---
        return False # Файлы не совпадают, не c-match

    # 2. Проверка покрытия для первой пары сопоставленных фрагментов
    cov_b1_t1, cov_t1_b1 = calculate_fragment_coverage(
        b_frag1_coords[0], b_frag1_coords[1], 
        t_frag1_coords[0], t_frag1_coords[1]
    )
    # --- DEBUG LOGGING ---
    # print(f"    [C_MATCH_DEBUG] Frag1 Pair: b_coords={b_frag1_coords}, t_coords={t_frag1_coords}")
    # print(f"    [C_MATCH_DEBUG] Coverage(B1, T1): {cov_b1_t1:.4f}, Coverage(T1, B1): {cov_t1_b1:.4f}, Threshold: {threshold}")
    # --- END DEBUG LOGGING ---
    if not (cov_b1_t1 >= threshold and cov_t1_b1 >= threshold):
        # --- DEBUG LOGGING ---
        # print(f"    [C_MATCH_DEBUG] Frag1 coverage failed. Result: False")
        # --- END DEBUG LOGGING ---
        return False

    # 3. Проверка покрытия для второй пары сопоставленных фрагментов
    cov_b2_t2, cov_t2_b2 = calculate_fragment_coverage(
        b_frag2_coords[0], b_frag2_coords[1], 
        t_frag2_coords[0], t_frag2_coords[1]
    )
    # --- DEBUG LOGGING ---
    # print(f"    [C_MATCH_DEBUG] Frag2 Pair: b_coords={b_frag2_coords}, t_coords={t_frag2_coords}")
    # print(f"    [C_MATCH_DEBUG] Coverage(B2, T2): {cov_b2_t2:.4f}, Coverage(T2, B2): {cov_t2_b2:.4f}, Threshold: {threshold}")
    # --- END DEBUG LOGGING ---
    if not (cov_b2_t2 >= threshold and cov_t2_b2 >= threshold):
        # --- DEBUG LOGGING ---
        # print(f"    [C_MATCH_DEBUG] Frag2 coverage failed. Result: False")
        # --- END DEBUG LOGGING ---
        return False
        
    # --- DEBUG LOGGING ---
    # print(f"    [C_MATCH_DEBUG] All coverages passed. Result: True")
    # --- END DEBUG LOGGING ---
    return True # Все условия выполнены

def extract_task_id_from_path(file_path_str):
    """
    Извлекает task_id из строки пути к файлу.
    Ожидаемый формат: .../extracted_solutions/ГОД/ID_ЗАДАЧИ/ИМЯ_ПОЛЬЗОВАТЕЛЯ/ФАЙЛ.py
    Возвращает task_id как строку или None, если извлечь не удалось.
    """
    try:
        # Ищем часть пути после 'extracted_solutions/'
        # Это сделает путь относительным к 'extracted_solutions/'
        # например '2017/5719039502450688/Amzaz/q3.py'
        # или 'extracted_solutions/2017/5719039502450688/Amzaz/q3.py' если extracted_solutions в начале
        
        path_parts = pathlib.Path(file_path_str).parts
        # Ищем индекс 'extracted_solutions'
        try:
            idx_extracted_solutions = path_parts.index('extracted_solutions')
            # task_id должен быть через один элемент после 'extracted_solutions' (idx + 2)
            # path_parts[idx_extracted_solutions + 1] будет год
            # path_parts[idx_extracted_solutions + 2] будет task_id
            if len(path_parts) > idx_extracted_solutions + 2:
                return path_parts[idx_extracted_solutions + 2]
        except ValueError:
            # Если 'extracted_solutions' не найдено, возможно, путь уже внутри этой структуры
            # или имеет другой формат. Попробуем другой подход, если путь абсолютный и глубокий.
            # Это менее надежно, но может сработать для путей из mock данных.
            # /.../PythonCloneBenchmark/extracted_solutions/2017/TASK_ID/...
            # TASK_ID обычно 4-й с конца, если считать от имени файла.
            # q3.py (-1), Amzaz (-2), 5719039502450688 (-3), 2017 (-4) , extracted_solutions (-5)
            if len(path_parts) >= 5 and path_parts[-5] == 'extracted_solutions':
                 return path_parts[-3] # task_id
        
        # print(f"[EXTRACT_TASK_ID_DEBUG] Не удалось извлечь task_id из пути: {file_path_str}")
        return None
    except Exception as e:
        # print(f"[EXTRACT_TASK_ID_DEBUG] Ошибка при извлечении task_id из {file_path_str}: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Оценка результатов детектора клонов относительно эталонного бенчмарка.")
    parser.add_argument("--benchmark_csv", type=str, required=True, help="Путь к CSV файлу эталонного бенчмарка (например, clones_2017.csv).")
    parser.add_argument("--tool_db", type=str, required=True, help="Путь к файлу БД SQLite с результатами работы детектора.")
    parser.add_argument("--threshold", type=float, default=0.7, help="Порог покрытия для c-match (по умолчанию 0.7).")
    parser.add_argument("--tool_table_name", type=str, default="detected_clones", help="Имя таблицы в БД с результатами детектора (по умолчанию 'detected_clones').")
    
    args = parser.parse_args()

    # --- Убираем или комментируем тестовый блок для основной работы ---
    # print("--- Тестирование calculate_c_match ---")
    # ... (тестовый блок как был)
    # print("--- Конец Тестового блока ---")

    print(f"Загрузка эталонного бенчмарка из: {args.benchmark_csv}")
    if not os.path.exists(args.benchmark_csv):
        print(f"Ошибка: Файл эталонного бенчмарка не найден: {args.benchmark_csv}")
        return
    try:
        script_dir = pathlib.Path(__file__).parent.resolve()
        print(f"Директория скрипта: {script_dir}")
        
        benchmark_df = pd.read_csv(args.benchmark_csv)

        def resolve_benchmark_path(p_str):
            # Пути в benchmark_csv типа '../extracted_solutions/...' относительно директории скрипта
            return str(script_dir.joinpath(p_str).resolve())

        benchmark_df['file1_path'] = benchmark_df['file1_path'].apply(resolve_benchmark_path)
        benchmark_df['file2_path'] = benchmark_df['file2_path'].apply(resolve_benchmark_path)
        
        if not benchmark_df.empty:
            print(f"Пример разрешенного пути из эталона: {benchmark_df.iloc[0]['file1_path']}")

    except Exception as e:
        print(f"Ошибка при чтении или разрешении путей в эталонном CSV: {e}")
        return
    
    print(f"Заголовки в эталонном бенчмарке: {benchmark_df.columns.tolist()}")
    print(f"Загружено эталонных пар: {len(benchmark_df)}")

    print(f"Загрузка результатов детектора из БД: {args.tool_db}, таблица: {args.tool_table_name}")
    if not os.path.exists(args.tool_db):
        print(f"Ошибка: Файл БД детектора не найден: {args.tool_db}")
        return
    try:
        conn = sqlite3.connect(args.tool_db)
        tool_df = pd.read_sql_query(f"SELECT * FROM {args.tool_table_name}", conn)
        conn.close()

        def resolve_tool_path(p_str):
            # Пути в tool_df (из smart_mock_results) должны быть уже абсолютными
            # resolve() для абсолютного пути просто его нормализует
            # Если бы они были относительными, то их следовало бы разрешать относительно корня проекта или текущей рабочей директории
            return str(pathlib.Path(p_str).resolve()) 
            
        tool_df['file1_path'] = tool_df['file1_path'].apply(resolve_tool_path)
        tool_df['file2_path'] = tool_df['file2_path'].apply(resolve_tool_path)

        if not tool_df.empty:
            print(f"Пример разрешенного пути из tool_df: {tool_df.iloc[0]['file1_path']}")

    except Exception as e:
        print(f"Ошибка при чтении или разрешении путей в БД детектора: {e}")
        return

    print(f"Заголовки в результатах детектора (после нормализации путей): {tool_df.columns.tolist()}")
    print(f"Загружено пар от детектора: {len(tool_df)}")

    # Извлечение task_id для tool_df
    print("Извлечение task_id для результатов детектора...")
    tool_df['task_id'] = tool_df['file1_path'].apply(extract_task_id_from_path)
    
    # Проверим, сколько task_id удалось извлечь
    valid_task_ids_in_tool_df = tool_df['task_id'].notna().sum()
    print(f"Успешно извлечено task_id для {valid_task_ids_in_tool_df} из {len(tool_df)} пар детектора.")
    if valid_task_ids_in_tool_df == 0 and len(tool_df) > 0:
        print("Предупреждение: Не удалось извлечь task_id ни для одной пары из детектора. Сопоставление по task_id будет неэффективным.")
        # Можно здесь либо остановить, либо продолжить без фильтрации по task_id,
        # но пользователь просил "самый оптимизированный", так что это проблема.

    matched_benchmark_indices = set()
    used_tool_indices = set()

    print("\nНачинаем сопоставление клонов...")
    # Используем tqdm для прогресс-бара, если он установлен
    try:
        from tqdm import tqdm
        benchmark_iterable = tqdm(benchmark_df.iterrows(), total=len(benchmark_df), desc="Сопоставление эталонных клонов")
    except ImportError:
        print("Библиотека tqdm не найдена, прогресс-бар не будет отображаться.")
        benchmark_iterable = benchmark_df.iterrows()

    for b_idx, benchmark_clone_row in benchmark_iterable:
        # ---- TEMPORARY LIMITER FOR DEBUGGING ----
        # if b_idx >= 20000: # Обработаем только первые N строк для отладки
        #     print(f"[DEBUG] Достигнут лимит обработки benchmark_df в {b_idx} строк для отладки.")
        #     break
        # ---- END TEMPORARY LIMITER ----

        b_task_id = str(benchmark_clone_row['task_id']) # Убедимся, что тип task_id совпадает

        # Фильтруем tool_df по task_id и по тем, что еще не использованы
        # Важно: tool_df['task_id'] также должен быть строкой для корректного сравнения
        candidate_tool_clones_df = tool_df[
            (tool_df['task_id'].astype(str) == b_task_id) & \
            (~tool_df.index.isin(used_tool_indices))
        ]

        for t_idx, tool_clone_row in candidate_tool_clones_df.iterrows():
            # tool_clone_row уже является словарем (pd.Series), когда получаем из iterrows()
            # --- DEBUG LOGGING ---
            # print(f"[MAIN_LOOP_DEBUG] Comparing B_IDX={b_idx} (Task: {b_task_id}, B1_Path: {benchmark_clone_row['file1_path']}) with T_IDX={t_idx} (Task: {tool_clone_row['task_id']}, T1_Path: {tool_clone_row['file1_path']})")
            # --- END DEBUG LOGGING ---
            if calculate_c_match(benchmark_clone_row, tool_clone_row, args.threshold):
                matched_benchmark_indices.add(b_idx)
                used_tool_indices.add(t_idx)
                break # Переходим к следующему эталонному клону

    TP = len(matched_benchmark_indices)
    total_benchmark_clones = len(benchmark_df)
    FN = total_benchmark_clones - TP
    
    total_tool_clones = len(tool_df)
    FP = total_tool_clones - len(used_tool_indices)

    # Расчет метрик
    precision = TP / (TP + FP) if (TP + FP) > 0 else 0
    recall = TP / (TP + FN) if (TP + FN) > 0 else 0
    f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

    print("\n--- Результаты оценки ---")
    print(f"Всего эталонных пар: {total_benchmark_clones}")
    print(f"Всего обнаруженных пар детектором: {total_tool_clones}")
    print(f"Порог c-match: {args.threshold}")
    print("-------------------------")
    print(f"True Positives (TP):  {TP}")
    print(f"False Positives (FP): {FP}")
    print(f"False Negatives (FN): {FN}")
    print("-------------------------")
    print(f"Precision: {precision:.4f}")
    print(f"Recall:    {recall:.4f}")
    print(f"F1-Score:  {f1_score:.4f}")
    print("-------------------------")

if __name__ == "__main__":
    main() 