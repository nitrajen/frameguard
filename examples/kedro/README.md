# dfguard Kedro example

A Kedro pipeline (`orders_pipeline`) with two nodes: enrich raw orders and
summarise by customer. dfguard is armed in `__init__.py` so every node function
is enforced automatically, no decorator required.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Run

```bash
kedro run
```

Run from the `kedro/` directory. The pipeline reads `data/raw_orders.csv` via
the catalog and writes outputs to memory. If the input file has an unexpected
schema, dfguard raises at the first node call before any processing starts.
