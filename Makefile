PY ?= python3
AS_OF ?= 2026-10-08T23:00:00Z

test:
	$(PY) -m unittest discover -s tests -t . -v

sample:
	rm -f output/sla_monitor.db
	$(PY) -m sla_monitor.pipeline --registry data/sample/registry.csv --input data/sample/events.csv --as-of $(AS_OF)
	$(PY) -m sla_monitor.reconcile --expected data/sample/expected --out output/reconciliation_report.txt
	$(PY) -m sla_monitor.report --out-dir output

regen-sample:
	$(PY) -m sla_monitor.generate_data --out data/sample --seed 42 --datasets 5 --days 8

bench:
	$(PY) -m sla_monitor.benchmark --rows 100000 --datasets 50
