import argparse
import sys
from pathlib import Path

from .core.builder import build_benchmark
from .core.evaluator import evaluate, evaluate_with_details, write_eval_report
from .adapters.ccaligner import convert_ccaligner_to_tool_csv
from .adapters.mock_detector import generate_mock_from_benchmark
from .adapters.nil import convert_nil_to_tool_csv
from .core.qc import qc_benchmark, write_qc_report


def main(argv=None):
    argv = argv or sys.argv[1:]
    parser = argparse.ArgumentParser(prog="pcbench", description="Python Clone Benchmark CLI")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # build subcommand
    p_build = subparsers.add_parser("build", help="Собрать эталон из GCJ CSV")
    p_build.add_argument("--year", required=True, help="Год, например 2017")
    p_build.add_argument("--input-csv", required=True, help="Путь к gcj<year>.csv")
    p_build.add_argument("--extracted-dir", default="extracted_solutions", help="Куда сохранять .py исходники")
    p_build.add_argument("--output-dir", default="benchmark_output", help="Куда сохранять CSV с парами клонов")
    p_build.add_argument("--limit-tasks", default=None, help="Список task_id через запятую")
    p_build.add_argument("--granularity", choices=["auto", "file", "function"], default="auto", help="Гранулярность фрагмента")
    p_build.add_argument("--min-lines", type=int, default=5, help="Минимальный размер фрагмента (в строках)")
    p_build.add_argument("--shard-by-task", action="store_true", help="Писать отдельный CSV на задачу")
    p_build.add_argument("--gzip", action="store_true", help="Сжимать CSV в gzip")
    p_build.add_argument("--main-names", default=None, help="Через запятую: список имён главных функций для приоритета")
    p_build.add_argument("--parallel-workers", type=int, default=0, help="Параллельная генерация по задачам (только с --shard-by-task)")
    p_build.add_argument("--annotate-clone-type", action="store_true", help="Добавить колонку с типом клона (Type-1..Type-4)")

    # eval subcommand
    p_eval = subparsers.add_parser("eval", help="Оценить результаты детектора относительно эталона")
    p_eval.add_argument("--benchmark-csv", required=True, help="Путь к CSV эталона")
    p_eval.add_argument("--tool-csv", required=True, help="Путь к CSV результатов инструмента")
    p_eval.add_argument("--metric", choices=["c", "sc", "fc"], default="c", help="Метрика совпадения")
    p_eval.add_argument("--threshold", type=float, default=0.7, help="Порог покрытия (0..1)")
    p_eval.add_argument("--epsilon", type=float, default=1e-10, help="Порог для fc-match")
    p_eval.add_argument("--report-dir", default=None, help="Директория для сохранения отчёта (summary.md + сэмплы)")

    # adapt-ccaligner subcommand
    p_cc = subparsers.add_parser("adapt-ccaligner", help="Конвертировать вывод CCAligner в формат tool CSV")
    p_cc.add_argument("--ccaligner-csv", required=True, help="Путь к clones.csv из CCAligner")
    p_cc.add_argument("--extracted-dir", required=True, help="Директория extracted_solutions")
    p_cc.add_argument("--year", required=True, help="Год (для индексации файлов)")
    p_cc.add_argument("--output-csv", required=True, help="Куда сохранить tool CSV")
    p_cc.add_argument("--assume-1-indexed", action="store_true", help="Если строки в CCAligner с 1")

    # adapt-nil subcommand
    p_nil = subparsers.add_parser("adapt-nil", help="Конвертировать вывод NIL в формат tool CSV")
    p_nil.add_argument("--nil-csv", required=True, help="CSV, который выводит NIL")
    p_nil.add_argument("--extracted-dir", required=True, help="Корень extracted_solutions, на котором запускался NIL")
    p_nil.add_argument("--output-csv", required=True, help="Куда сохранить tool CSV")
    p_nil.add_argument(
        "--zero-indexed",
        action="store_true",
        help="Если в CSV NIL уже 0-индексация (по умолчанию считается 1-индексация)",
    )

    # gen-mock subcommand
    p_mock = subparsers.add_parser("gen-mock", help="Сгенерировать псевдо-вывод детектора из эталона")
    p_mock.add_argument("--benchmark-csv", required=True)
    p_mock.add_argument("--output-csv", required=True)
    p_mock.add_argument("--take-first-n", type=int, default=100)
    p_mock.add_argument("--add-false-m", type=int, default=20)

    # qc subcommand
    p_qc = subparsers.add_parser("qc", help="Проверить эталон: валидность путей/координат и базовую статистику")
    p_qc.add_argument("--benchmark-csv", required=True)
    p_qc.add_argument("--project-root", default=str(Path(__file__).resolve().parents[1]))
    p_qc.add_argument("--report-dir", default=None)

    args = parser.parse_args(argv)

    if args.command == "build":
        limit = set(args.limit_tasks.split(",")) if args.limit_tasks else None
        build_benchmark(
            year=str(args.year),
            input_csv_path=Path(args.input_csv),
            extracted_dir=Path(args.extracted_dir),
            output_dir=Path(args.output_dir),
            limit_tasks=limit,
            granularity=args.granularity,
            min_lines=args.min_lines,
            shard_by_task=args.shard_by_task,
            gzip_output=args.gzip,
            main_names=[s.strip() for s in args.main_names.split(',')] if args.main_names else None,
            parallel_workers=args.parallel_workers,
            annotate_clone_type=args.annotate_clone_type,
        )
        return 0
    elif args.command == "eval":
        if args.report_dir:
            metrics, tp, fp, fn = evaluate_with_details(
                benchmark_csv=Path(args.benchmark_csv),
                tool_csv=Path(args.tool_csv),
                metric=args.metric,
                threshold=args.threshold,
                epsilon=args.epsilon,
                sample_k=100,
            )
            out_dir = Path(args.report_dir)
            write_eval_report(out_dir, metrics, tp, fp, fn, args.metric, args.threshold, args.epsilon)
            print(f"Report written to {out_dir}")
        else:
            res = evaluate(
                benchmark_csv=Path(args.benchmark_csv),
                tool_csv=Path(args.tool_csv),
                metric=args.metric,
                threshold=args.threshold,
                epsilon=args.epsilon,
            )
            print(
                "\n".join(
                    [
                        f"Metric: {args.metric}",
                        f"TP={res['TP']} FP={res['FP']} FN={res['FN']}",
                        f"Precision={res['precision']:.4f}",
                        f"Recall={res['recall']:.4f}",
                        f"F1={res['f1']:.4f}",
                        f"Totals: benchmark={res['total_benchmark']} tool={res['total_tool']}",
                    ]
                )
            )
        return 0
    elif args.command == "adapt-ccaligner":
        out = convert_ccaligner_to_tool_csv(
            ccaligner_csv=Path(args.ccaligner_csv),
            extracted_dir=Path(args.extracted_dir),
            year=str(args.year),
            output_csv=Path(args.output_csv),
            assume_1_indexed_input=bool(args.assume_1_indexed),
        )
        print(f"Converted to {out}")
        return 0
    elif args.command == "gen-mock":
        out = generate_mock_from_benchmark(
            benchmark_csv=Path(args.benchmark_csv),
            output_csv=Path(args.output_csv),
            take_first_n=args.take_first_n,
            add_false_m=args.add_false_m,
        )
        print(f"Mock detector CSV written to {out}")
        return 0
    elif args.command == "adapt-nil":
        out = convert_nil_to_tool_csv(
            nil_csv=Path(args.nil_csv),
            extracted_dir=Path(args.extracted_dir),
            output_csv=Path(args.output_csv),
            assume_1_indexed_input=not args.zero_indexed,
        )
        print(f"Converted to {out}")
        return 0
    elif args.command == "qc":
        metrics = qc_benchmark(Path(args.benchmark_csv), Path(args.project_root))
        if args.report_dir:
            p = write_qc_report(Path(args.report_dir), metrics)
            print(f"QC report written to {p}")
        else:
            for k, v in metrics.items():
                print(f"{k}: {v}")
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
