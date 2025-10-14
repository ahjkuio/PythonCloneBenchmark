# PythonCloneBenchmark

PythonCloneBenchmark - это модульный бенчмарк и система оценки детекторов клонов Python‑кода. Проект строит эталонные пары клонов на основе реальных решений Google Code Jam (GCJ) и позволяет сравнивать вывод любых детекторов по метрикам из работы Svajlenko & Roy, ICSME’15 (c/sc/fc‑match, Precision/Recall/F1).

**Готовый бенчмарк:** архивы с полным эталоном и отчётами лежат на Яндекс.Диске — <https://disk.yandex.ru/d/syutKpM8Z0I7hA>.

## Почему это работает
- **Реальные данные.** Решения одной GCJ‑задачи считаются семантическими клонами (Type‑4). Такой подход даёт разнообразные реализации, а не синтетические примеры.
- **Воспроизводимый пайплайн.** Вся сборка автоматизирована: чтение CSV, выбор главной функции, генерация пар, QC отчёты.
- **Метрики принятого стандарта.** Используем c/sc/fc‑match и классические TP/FP/FN → Precision/Recall/F1, полностью совместимые с BigCloneEval.
- **Модульность.** Ядро, адаптеры, CLI и тесты разнесены по отдельным модулям: легко подключать новые источники данных и инструменты.
- **Аннотация типов клонов.** Опционально оцениваем схожесть пары по типологии Type‑1..Type‑4 (эвристика на основе токенов и структуры AST) для быстрой аналитики.

## Структура репозитория
```
pcbench/
  core/          # чтение GCJ, выбор фрагментов, генерация эталона, метрики, QC
  adapters/      # конвертеры для CCAligner, NIL, SourcererCC и NiCad
  cli.py         # точка входа CLI (команда `pcbench`)
scripts/         # тонкие обёртки для запуска CLI через `python .../scripts/build.py`
tests/           # pytests для ключевых компонентов и адаптеров
pyproject.toml   # упаковка, entrypoint `pcbench`, зависимости
requirements.txt # минимальный список зависимостей
README.md        # этот файл
```

## Установка
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .[test]
```
После установки доступна команда `pcbench`.

## Быстрый старт (мини-набор)
Все команды выполняются из корня репозитория.

### 1. Сборка эталона
```bash
pcbench build \
  --year 2017 \
  --input-csv gcj-dataset/small_gcj2017.csv \
  --output-dir benchmark_output \
  --extracted-dir extracted_solutions \
  --granularity auto \
  --min-lines 5 \
  --annotate-clone-type \
  --gzip
```
Что происходит:
- извлекаем Python‑решения из CSV;
- выделяем главную функцию (AST → alias → regex → fallback);
- генерируем пары клонов внутри каждой задачи;
- при `--annotate-clone-type` добавляем колонку `clone_type` (Type‑1..Type‑4).

### 2. QC (проверка качества эталона)
```bash
pcbench qc \
  --benchmark-csv benchmark_output/clones_2017.csv.gz \
  --report-dir benchmark_output/qc_report
```
`benchmark_output/qc_report/benchmark_qc_summary.md` покажет число строк, проблемные пути и статистику длин фрагментов.

### 3. Генерация псевдо‑детектора и оценка
```bash
pcbench gen-mock \
  --benchmark-csv benchmark_output/clones_2017.csv.gz \
  --output-csv data/mock_tool.csv \
  --take-first-n 100 --add-false-m 20

pcbench eval \
  --benchmark-csv benchmark_output/clones_2017.csv.gz \
  --tool-csv data/mock_tool.csv \
  --metric c --threshold 0.7 \
  --report-dir benchmark_output/eval_report_demo
```
В каталоге `benchmark_output/eval_report_demo/` появится `summary.md` (TP/FP/FN, Precision/Recall/F1) и CSV с примерами TP/FP/FN.

### 4. Подключение реальных инструментов
- **CCAligner → pcbench** ([репозиторий](https://github.com/cloudyy74/CCAligner))
  ```bash
  pcbench adapt-ccaligner \
    --ccaligner-csv path/to/clones.csv \
    --extracted-dir extracted_solutions \
    --year 2017 \
    --output-csv data/tool_ccaligner.csv \
    --assume-1-indexed
  ```
- **NIL → pcbench** ([репозиторий](https://github.com/kusumotolab/NIL))
  ```bash
  pcbench adapt-nil \
    --nil-csv path/to/result.csv \
    --extracted-dir extracted_solutions \
    --output-csv data/tool_nil.csv
  ```
- **SourcererCC → pcbench** ([репозиторий](https://github.com/Mondego/SourcererCC))
  ```bash
  # запуск детектора (пример для file-level tokeniser)
  git clone https://github.com/Mondego/SourcererCC ../SourcererCC
  # подготовьте список проектов (архивы или каталоги) и выполните tokenizer + clone-detector
  # в результате появится файл clones_index_WITH_FILTER.db или results.pairs

  pcbench adapt-sourcerercc \
    --pairs-file path/to/clones_index_WITH_FILTER.db \
    --project-root /path/to/PythonCloneBenchmark \
    --output-csv data/tool_sourcerercc.csv
  ```
- **NiCad → pcbench** ([репозиторий](https://github.com/CordyJ/Open-NiCad))
  ```bash
  git clone https://github.com/CordyJ/Open-NiCad ../Open-NiCad
  # запустите NiCad на каталоге extracted_solutions (пример: nicad4 functions python ...)
  # на выходе появится файл ..._functions-blind-clones.xml

  pcbench adapt-nicad \
    --clusters-xml path/to/extracted_solutions_functions-blind-clones.xml \
    --project-root /path/to/PythonCloneBenchmark \
    --output-csv data/tool_nicad.csv
  ```

### Быстрая проверка на небольшом наборе
1. Скачайте исходные CSV GCJ:
   ```bash
   git clone https://github.com/Jur1cek/gcj-dataset ../gcj-dataset
   tar -xjf ../gcj-dataset/gcj2017.csv.tar.bz2 -C ../gcj-dataset
   ```
2. Сформируйте мини-CSV (например, по двум задачам):
   ```bash
   python3 - <<'PY'
   import pandas as pd
   src = "../gcj-dataset/gcj2017.csv"
   out = "../gcj-dataset/gcj2017_small.csv"
   tasks = {"5719039502450688", "5697460110360576"}
   chunks = []
   for chunk in pd.read_csv(src, chunksize=1000):
       sub = chunk[chunk['task'].astype(str).isin(tasks)]
       if not sub.empty:
           chunks.append(sub)
           if sum(len(c) for c in chunks) >= 100:
               break
  small = pd.concat(chunks).groupby('task').head(20)
  small.to_csv(out, index=False)
  PY
  ```
3. Соберите эталон и QC:
   ```bash
   pcbench build --year 2017 \
     --input-csv ../gcj-dataset/gcj2017_small.csv \
     --output-dir benchmark_output \
     --extracted-dir extracted_solutions \
     --granularity auto --min-lines 5 --annotate-clone-type
   ```
   ```bash
   pcbench qc --benchmark-csv benchmark_output/clones_2017.csv.gz
   ```
4. SourcererCC: добавьте путь к `extracted_solutions` в `tokenizers/file-level/paths.txt`, запустите `python tokenizer.py zip` и `python controller.py`. Полученный `clones_index_WITH_FILTER.db` адаптируйте командой из абзаца выше.
5. NiCad: в каталоге `Open-NiCad` выполните `./nicad4 functions python extracted_solutions ni_result` (пример). Файл `ni_result/extracted_solutions_functions-blind-clones.xml` адаптируйте через `pcbench adapt-nicad`.
6. NIL: соберите и запустите JAR (см. README проекта NIL, пример: `java -jar NIL-all.jar --src extracted_solutions --language python --min-line 5 --output data/nil.csv`). Готовый CSV адаптируйте через `pcbench adapt-nil`.
7. После адаптации можно запустить `pcbench eval` и, при необходимости, `python scripts/eval_by_type.py --benchmark ... --tool ...`, чтобы посмотреть метрики и распределение по типам.

Эти шаги дают быстрый прогон и подтверждают, что адаптеры и пайплайн работают до запуска на полном наборе (105 млн пар).

## Как устроено ядро
- `pcbench/core/gcj_reader.py` — автоматическое определение формата (CSV/TSV), фильтрация по году/задачам.
- `pcbench/core/code_fragments.py` — подбор главной функции: учитываем `if __name__ == "__main__"`, alias `main = solve`, методы `solver = Solver(); solver.run()`, поддержка `async def`, fallback на скрипт.
- `pcbench/core/builder.py` — генерация пар клонов, шардирование по задачам, gzip, опциональная аннотация типами.
- `pcbench/core/clone_types.py` — Type‑1..Type‑4 классификация (лексический и структурный анализ, SequenceMatcher).
- `pcbench/core/evaluator.py` — реализация c/sc/fc‑match, расчёт TP/FP/FN, Precision/Recall/F1.
- `pcbench/core/qc.py` — базовые проверки (существование файлов, валидность координат, статистика длины фрагмента).

## Пайплайн под микроскопом

Ниже — как именно формируется строка в эталоне. Все ссылки на код включают номер строки (например, `builder.py:55`).

1. **Чтение CSV (`gcj_reader.py:55`).** Функция `read_gcj_csv` открывает `gcj2017.csv`, определяет разделитель (`csv.Sniffer`) и проверяет, что есть столбцы `file`, `flines`, `task`, `username`, `year`. Каждая строка превращается в словарь вида:
   ```python
   {
       "year": "2017",
       "task": "5719039502450688",
       "username": "Amzaz",
       "file": "q3.py",
       "flines": "#! /usr/bin/python2.7\n..."
   }
   ```
2. **Сохранение исходников (`builder.py:62-67`).** Для каждой записи создаётся каталог `extracted_solutions/<year>/<task>/<user>/` и туда пишется исходный файл (с заменой `/` на `_` в имени, чтобы не ломать структуру).
3. **Выбор фрагмента (`builder.py:71-94`).**
   - `get_function_boundaries_ex` (см. `code_fragments.py`) пытается найти функцию, вызываемую из `if __name__ == '__main__'`. Если не вышло — бегает регуляркой `^\s*(?:async\s+)?def` и, в самом крайнем случае, берёт весь файл.
   - Возвращается тройка `(start, end, reason)`. Функция `clamp_fragment` затем ограничивает координаты диапазоном `[0, num_lines - 1]`, подменяя отрицательные значения и обрезая выход за конец файла. Параллельно ведём счётчик стратегий (`strategy_counts`) — он попадает в summary.
4. **Накопление по задачам (`builder.py:86-95`).** Для каждого `task` собираем список словарей `{'path', 'start', 'end', 'granularity'…}`. Элементов должно быть минимум два, иначе пара не образуется.
5. **Генерация пар (`builder.py:135-178`).** Внутренний двойной цикл `for i in range(n): for j in range(i + 1, n)` перебирает все комбинации решений внутри задачи. Индексы гарантированно разные (`j > i`), поэтому мы никогда не сравниваем элемент сам с собой. Однако в исходном CSV может быть несколько записей одного и того же участника с одинаковым именем файла — это разные попытки, которые мы сохраняем все. Файл на диске перезаписывается последней версией, но координаты (`start`, `end`) в памяти остаются разными, поэтому в эталоне появится несколько строк с одним и тем же `file_path`, но разными диапазонами. Так мы отражаем «вертикальные» совпадения между несколькими отправками участника.
6. **Аннотация типов (`builder.py:158-163` + `clone_types.py`).** Если указали `--annotate-clone-type`, берём строки кода через `_extract_fragment` и передаём в `classify_clone_pair`. Классификатор делает ровно четыре шага:
   1. `_normalize_tokens(structural=False)` — удаляем комментарии/пробелы. Если строки совпали, значит это Type‑1 (дословный клон).
   2. `_normalize_tokens(structural=True)` — заменяем все идентификаторы на `ID`, числа на `NUM`, строки на `STR`. Совпадение здесь → Type‑2 (переименование).
   3. Если структуры различаются, считаем похожесть `SequenceMatcher`; порог `type3_min_similarity=0.8` задаёт Type‑3 (near-miss).
   4. Остальное попадает в Type‑4.
7. **Запись CSV (`builder.py:135-178`).** В результате каждая строка содержит пути, интервалы, `task_id`, `clone_type`. Если `--gzip`, файл сжимается (`clones_<year>.csv.gz`).

### Пример на мини-наборе

Чтобы получить руками демонстрационный эталон, достаточно ограничиться парой задач (см. раздел “Быстрая проверка” для создания `gcj2017_small.csv`):

```bash
pcbench build \
  --year 2017 \
  --input-csv ../gcj-dataset/gcj2017_small.csv \
  --output-dir demo_output \
  --extracted-dir demo_extracted \
  --granularity auto --min-lines 5 --annotate-clone-type --gzip
```

Получаем четырёхстрочное `demo_output/clones_2017.csv.gz`. Например, для задачи `5719039502450688` попадает пара:

```
demo_extracted/2017/5719039502450688/Amzaz/q3.py:21-26
demo_extracted/2017/5719039502450688/mth/C.py:11-46
clone_type = type4
```

Фрагмент `Amzaz` — короткая обёртка над `inner_solve`, `mth` реализует цикл перебора состояний дракона и рыцаря. Токены различаются, поэтому Type‑4. В том же CSV есть и Type‑3: две строки с путём `Amzaz/q2.py`. Это две разные записи из CSV (разные попытки участника), каждая дала новую строку в `python_by_task`. Их диапазоны почти совпадают, поэтому `SequenceMatcher` возвращает ~0.87, и классификатор помечает одну пару как Type‑3.

### Как мы сравниваем с инструментами

`pcbench` ожидает вход в виде *tool CSV* — файла с колонками `file1_path,file1_start,file1_end,file2_path,file2_start,file2_end`. Адаптеры преобразуют родной формат детектора в этот вид:

- `pcbench/adapters/nil.py` — CSV NIL → tool CSV (поправка индексации, нормализация путей).
- `pcbench/adapters/sourcerercc.py` — построчно читает output SourcererCC (`clones_index_WITH_FILTER.db`, `results.pairs`). Каждая строчка разбивается на путь и координаты, которые приводятся к `project_root`.
- `pcbench/adapters/nicad.py` — разбирает NiCad XML, перебирает `<source>` внутри `<clone>` и генерирует пары.

Важно: внешние детекторы не присваивают тип клона — мы сопоставляем их пары с эталоном и используем колонку `clone_type` из `clones_*.csv`.

**Метрики (`evaluator.py`).** После адаптации сверху у нас две таблицы: эталон (`clones_*.csv`) и tool CSV. Дальше идут три варианта сопоставления:

1. `_c_match` — проверяем, что инструмент покрывает эталонный диапазон минимум на `threshold` (по умолчанию 0.7). Это односторонняя проверка «инструмент достаточно длинный».
2. `_sc_match` — симметричная версия: оба покрытия (эталон инструментом и инструмент эталоном) должны быть ≥ порога. Помогает исключить случаи, когда инструмент репортит слишком короткие фрагменты.
3. `_fc_match` — дополнительно проверяет, что инструмент *не* захватил слишком много (каждый фрагмент инструмента перекрыт эталоном хотя бы на `epsilon`, по умолчанию 1e-10). Используем, когда детектор склонен репортить огромные куски.

Функция `evaluate` для каждой эталонной пары ищет совпадения в tool CSV (без учёта порядка, пары сравниваются как наборы путей). Результат — стандартные метрики:

- **TP** (true positive) — эталонная пара, которая проходит выбранное сравнение (например, `_c_match`).
- **FP** (false positive) — пара из tool CSV, которой не нашлось эквивалента в эталоне.
- **FN** (false negative) — эталонная пара, которую инструмент не покрыл.
- **Precision = TP / (TP + FP)** — доля истинных срабатываний среди всех ответов инструмента.
- **Recall = TP / (TP + FN)** — доля найденных эталонных пар.
- **F1** — гармоническое среднее между Precision и Recall.

`pcbench eval` печатает эти числа в консоль, а при `--report-dir` также сохраняет Markdown `summary.md` и примеры `tp.csv`/`fp.csv`/`fn.csv` (полезно для презентаций).
Отдельный скрипт `scripts/eval_by_type.py` берёт эталон и tool CSV и пересчитывает TP/FN по каждому `clone_type`, печатая Markdown-таблицу `Type | total | TP | FN | recall`. Так можно быстро понять, какие типы клонов покрывает конкретный детектор.
  - `evaluate` и `evaluate_with_details` перебирают эталонные строки, ищут соответствия в tool CSV (пары сравниваются без учёта порядка путей) и считают `TP/FP/FN`, Precision/Recall/F1. При `--report-dir` результаты сохраняются в `summary.md`, `tp.csv`, `fp.csv`, `fn.csv`.

## Почему аннотация типов полезна
- Помогает быстро понять «насколько близки» две реализации: Type‑1 (чисто формат), Type‑2 (переименование), Type‑3 (near-miss), Type‑4 (семантика).
- Распределение по типам пишется в консоль/summary: легко анализировать результаты инструмента по типам клонов.

## Тестирование
```bash
pytest
```
Сейчас запускается 21 тест: чтение GCJ, выбор функции, классификация типов, адаптеры CCAligner/NIL/SourcererCC/NiCad.

## Данные
- Репозиторий содержит только код и лёгкие вспомогательные файлы.
- Полные выгрузки GCJ (например, `gcj-dataset/small_gcj2017.csv`) и результаты инструментов находятся вне репозитория и подключаются в командах через относительные пути.

## Дорожная карта
- Расширить QC (метрики по задачам, визуализация распределений).
- Исследовать более точные признаки для определения Type‑3/4 (по примеру Amain, AST‑Markov Chains).
- Добавить поддержку других лет GCJ и других языков (через расширения в `LANGUAGE_EXTENSIONS`).

Если есть вопросы или предложения, открывайте issue/PR или пишите в чате проекта. Нам важно, чтобы этот бенчмарк стал стандартом для оценки Python‑клонов так же, как BigCloneEval для Java.
