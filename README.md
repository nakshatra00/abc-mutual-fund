# Corporate Bond Fund Portfolio Pipeline

Reproducible pipeline to ingest six AMFI Corporate Bond Fund portfolio sheets (as of 31-Jul-2025), standardize, enrich, and analyze.

## Structure
- `config/`: sheet hints, schema alias map, bucket edges, rating normalization
- `data/raw/2025-07-31/`: place the 6 Excel files here
- `src/`: Python modules for ingest → normalize → enrich → aggregate
- `output/2025-07-31/`: aggregates.xlsx + analysis.md
- `processed/`: merged_holdings.parquet after run

## Install
```bash
pip install -r requirements.txt
```

## Run
```bash
python -m src.main   --asof 2025-07-31   --raw-dir data/raw/2025-07-31   --out-dir output/2025-07-31   --config-dir config
```

## Notes
- Units normalized to **Rs. in Lacs**.
- Maturity parsed from name if missing; AT1/Perpetual excluded from maturity buckets.
- Sovereign (G-Sec/SDL/T-Bill) tagged and assigned `SOVEREIGN` rating.
- Weighted-average yield computed over rows with YIELD; coverage reported.
