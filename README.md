# PythonCloneBenchmark

PythonCloneBenchmark — это модульный бенчмарк и система оценки детекторов клонов Python‑кода. Проект строит эталонные пары клонов на основе реальных решений Google Code Jam (GCJ) и позволяет сравнивать вывод любых детекторов по метрикам из работы Svajlenko & Roy, ICSME’15 (c/sc/fc‑match, Precision/Recall/F1).

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
  adapters/      # конвертеры для CCAligner, NIL и генератор псевдо-детектора
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
- **CCAligner → pcbench**
  ```bash
  pcbench adapt-ccaligner \
    --ccaligner-csv path/to/clones.csv \
    --extracted-dir extracted_solutions \
    --year 2017 \
    --output-csv data/tool_ccaligner.csv \
    --assume-1-indexed
  ```
- **NIL → pcbench**
  ```bash
  pcbench adapt-nil \
    --nil-csv path/to/result.csv \
    --extracted-dir extracted_solutions \
    --output-csv data/tool_nil.csv
  ```
Затем используйте `pcbench eval` как в пункте 3.

## Как устроено ядро
- `pcbench/core/gcj_reader.py` — автоматическое определение формата (CSV/TSV), фильтрация по году/задачам.
- `pcbench/core/code_fragments.py` — подбор главной функции: учитываем `if __name__ == "__main__"`, alias `main = solve`, методы `solver = Solver(); solver.run()`, поддержка `async def`, fallback на скрипт.
- `pcbench/core/builder.py` — генерация пар клонов, шардирование по задачам, gzip, опциональная аннотация типами.
- `pcbench/core/clone_types.py` — Type‑1..Type‑4 классификация (лексический и структурный анализ, SequenceMatcher).
- `pcbench/core/evaluator.py` — реализация c/sc/fc‑match, расчёт TP/FP/FN, Precision/Recall/F1.
- `pcbench/core/qc.py` — базовые проверки (существование файлов, валидность координат, статистика длины фрагмента).

## Почему аннотация типов полезна
- Помогает быстро понять «насколько близки» две реализации: Type‑1 (чисто формат), Type‑2 (переименование), Type‑3 (near-miss), Type‑4 (семантика).
- Распределение по типам пишется в консоль/summary: легко анализировать результаты инструмента по типам клонов.

## Тестирование
```bash
pytest
```
19 сценариев покрывают: чтение GCJ, выбор функции, метрики покрытий, адаптеры CCAligner/NIL, классификацию типов, сборку с аннотацией.

## Данные
- Репозиторий содержит только код и лёгкие вспомогательные файлы.
- Полные выгрузки GCJ (например, `gcj-dataset/small_gcj2017.csv`) и результаты инструментов находятся вне репозитория и подключаются в командах через относительные пути.

## Дорожная карта
- Расширить QC (метрики по задачам, визуализация распределений).
- Интегрировать NIL/CCAligner на полном наборе решений и собрать отчёты.
- Экспорт результатов в формат BigCloneEval.
- Исследовать более точные признаки для определения Type‑3/4 (по примеру Amain, AST‑Markov Chains).
- Добавить поддержку других лет GCJ и других языков (через расширения в `LANGUAGE_EXTENSIONS`).

Если есть вопросы или предложения, открывайте issue/PR или пишите в чате проекта. Нам важно, чтобы этот бенчмарк стал стандартом для оценки Python‑клонов так же, как BigCloneEval для Java.
