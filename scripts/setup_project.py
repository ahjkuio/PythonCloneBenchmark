import os
import argparse
import requests
import tarfile
import bz2
import shutil
import subprocess
from tqdm import tqdm

# Базовый URL для скачивания архивов GCJ
GCJ_ARCHIVE_BASE_URL = "https://github.com/Jur1cek/gcj-dataset/raw/master/"
# Директория для сохранения скачанных архивов (относительно корня проекта)
GCJ_ARCHIVES_ROOT_SUBDIR = "data/gcj_csv_archives"
# Директория для распакованных CSV файлов (относительно корня проекта)
GCJ_UNPACKED_ROOT_SUBDIR = "data/gcj_csv_unpacked"

# Другие основные директории проекта (относительно корня проекта)
PROJECT_DIRS = [
    "data/mock_detector_output",
    "data/tool_results",
    "extracted_solutions",
    "benchmark_output"
]

def ensure_project_directories(base_project_path):
    """Создает основные директории проекта, если они не существуют."""
    print("Проверка и создание структуры директорий проекта...")
    all_dirs_to_create = [
        os.path.join(base_project_path, GCJ_ARCHIVES_ROOT_SUBDIR),
        os.path.join(base_project_path, GCJ_UNPACKED_ROOT_SUBDIR)
    ]
    for proj_dir in PROJECT_DIRS:
        all_dirs_to_create.append(os.path.join(base_project_path, proj_dir))

    for dir_path in all_dirs_to_create:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
            print(f"Создана директория: {dir_path}")
        else:
            print(f"Директория уже существует: {dir_path}")

def download_and_unpack_gcj_csv(year, base_project_path):
    """
    Скачивает и распаковывает CSV файл GCJ для указанного года.
    Возвращает путь к распакованному CSV файлу или None в случае ошибки.
    """
    unpacked_csv_target_dir = os.path.join(base_project_path, GCJ_UNPACKED_ROOT_SUBDIR)
    archives_target_dir = os.path.join(base_project_path, GCJ_ARCHIVES_ROOT_SUBDIR)
    
    expected_csv_filename = f"gcj{year}.csv"
    expected_csv_path_in_unpacked = os.path.join(unpacked_csv_target_dir, expected_csv_filename)

    if os.path.exists(expected_csv_path_in_unpacked):
        print(f"Найден существующий распакованный CSV файл: {expected_csv_path_in_unpacked}")
        return expected_csv_path_in_unpacked

    print(f"CSV файл {expected_csv_path_in_unpacked} не найден. Попытка скачивания...")

    archive_filename = f"gcj{year}.csv.tar.bz2"
    archive_url = f"{GCJ_ARCHIVE_BASE_URL}{archive_filename}"
    archive_save_path = os.path.join(archives_target_dir, archive_filename)

    try:
        print(f"Скачивание {archive_url} в {archive_save_path}...")
        response = requests.get(archive_url, stream=True)
        response.raise_for_status() 
        total_size = int(response.headers.get('content-length', 0))
        
        with open(archive_save_path, 'wb') as f, tqdm(
            desc=archive_filename,
            total=total_size,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for chunk in response.iter_content(chunk_size=8192):
                size = f.write(chunk)
                bar.update(size)
        print("Скачивание завершено.")

        print(f"Распаковка {archive_save_path} в {unpacked_csv_target_dir}...")
        with bz2.open(archive_save_path, 'rb') as bz2f:
            with tarfile.open(fileobj=bz2f, mode='r|*') as tarf:
                member_to_extract = None
                for m in tarf.getmembers():
                    if m.name.endswith(f'{year}.csv'): 
                        member_to_extract = m
                        break
                if member_to_extract:
                    target_extraction_path = os.path.join(unpacked_csv_target_dir, os.path.basename(member_to_extract.name))
                    with tarf.extractfile(member_to_extract) as source, open(target_extraction_path, 'wb') as dest:
                        shutil.copyfileobj(source, dest)
                    print(f"Файл {member_to_extract.name} распакован в {target_extraction_path}")
                    # Опционально: удалить архив после успешной распаковки
                    # os.remove(archive_save_path)
                    # print(f"Архив {archive_save_path} удален.")
                    return target_extraction_path
                else:
                    print(f"Ошибка: Не найден {expected_csv_filename} внутри архива {archive_filename}.")
                    # Попытаемся удалить некорректный/пустой архив
                    if os.path.exists(archive_save_path):
                        os.remove(archive_save_path)
                        print(f"Удален некорректный архив: {archive_save_path}")
                    return None

    except requests.exceptions.RequestException as e:
        print(f"Ошибка скачивания: {e}")
    except tarfile.TarError as e:
        print(f"Ошибка распаковки tar: {e}")
    except bz2.BZ2Error as e:
        print(f"Ошибка распаковки bz2: {e}")
    except Exception as e:
        print(f"Непредвиденная ошибка при скачивании/распаковке: {e}")
    
    # Если произошла ошибка и файл архива мог остаться, удаляем его
    if os.path.exists(archive_save_path):
        try:
            os.remove(archive_save_path)
            print(f"Удален частично скачанный или поврежденный архив: {archive_save_path}")
        except OSError as e_del:
            print(f"Не удалось удалить архив {archive_save_path}: {e_del}")
    return None

def install_dependencies(base_project_path):
    """Устанавливает зависимости из requirements.txt."""
    print("\nУстановка зависимостей из requirements.txt...")
    requirements_path = os.path.join(base_project_path, "requirements.txt")
    if not os.path.exists(requirements_path):
        print(f"Файл {requirements_path} не найден. Пропустите установку зависимостей.")
        return False
    try:
        subprocess.check_call(['pip', 'install', '-r', requirements_path])
        print("Зависимости успешно установлены/обновлены.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при установке зависимостей: {e}")
        print("Пожалуйста, установите зависимости вручную, выполнив: pip install -r requirements.txt")
        return False
    except FileNotFoundError:
        print("Ошибка: команда 'pip' не найдена. Убедитесь, что Python и pip установлены и доступны в PATH.")
        print("Пожалуйста, установите зависимости вручную, выполнив: pip install -r requirements.txt")
        return False

def main():
    # Определяем базовый путь проекта (на один уровень выше директории scripts)
    base_project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    parser = argparse.ArgumentParser(description="Скрипт для первоначальной настройки проекта PythonCloneBenchmark.")
    parser.add_argument("--year", type=str, help="Год для скачивания данных GCJ (например, 2017). Можно указать несколько через запятую или 'all'.")
    parser.add_argument("--skip_dependencies", action='store_true', help="Пропустить установку зависимостей.")
    parser.add_argument("--skip_gcj_download", action='store_true', help="Пропустить скачивание данных GCJ.")
    
    args = parser.parse_args()

    if not args.skip_dependencies:
        install_dependencies(base_project_path)

    ensure_project_directories(base_project_path)

    if not args.skip_gcj_download:
        if args.year:
            years_to_download = []
            if args.year.lower() == 'all':
                # Укажем года, доступные в Jur1cek/gcj-dataset, до 2017, как обсуждалось
                years_to_download = [str(y) for y in range(2008, 2018)] 
            else:
                years_to_download = [y.strip() for y in args.year.split(',')]
            
            print(f"\nЗапрос на скачивание данных GCJ для года(лет): {', '.join(years_to_download)}")
            for year_str in years_to_download:
                if not year_str.isdigit() or not (2008 <= int(year_str) <= 2020) : # Проверка на корректность года
                     print(f"Предупреждение: Год '{year_str}' указан некорректно или выходит за пределы доступных (2008-2020). Пропуск.")
                     continue
                print(f"--- Обработка года {year_str} ---")
                download_and_unpack_gcj_csv(year_str, base_project_path)
        else:
            print("\nГод для скачивания данных GCJ не указан. Используйте --year ГОД для скачивания.")
            print("Например: --year 2017  или --year 2016,2017 или --year all (для 2008-2017)")
    else:
        print("\nСкачивание данных GCJ пропущено (согласно флагу --skip_gcj_download).")

    print("\nНастройка проекта завершена.")
    print(f"Убедитесь, что файл CSV для нужного года (например, gcj{args.year}.csv) находится в {os.path.join(base_project_path, GCJ_UNPACKED_ROOT_SUBDIR)}")

if __name__ == '__main__':
    main() 