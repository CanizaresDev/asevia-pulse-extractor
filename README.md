# HORSE — Daily Parquet → DuckDB → Google Sheets

This repo contains a Python script that:

1. Downloads a `.parquet` file from **DigitalOcean Spaces** (S3-compatible) _or_ uses a local Parquet file.
2. Uses **DuckDB** to extract/pivot counters into a tabular dataset.
3. Writes the result to a **Google Sheets** tab, clearing the tab before writing.

A **GitHub Actions** workflow runs the script daily.

---

## Output (Google Sheet columns)

The script writes these columns:

- `client` → `clientAccount.tradeName`
- `ingested_at` → full `ingestion_ts` timestamp (latest register per client/day)
- `date` → `CAST(ingestion_ts + INTERVAL 1 DAY AS DATE)` (YYYY-MM-DD)
- one column per counter source (from `counters`):
  - `accounting_entries_review_pending`
  - `accounting_assets_pending`
  - `banking_reconcile_pending`
  - `banking_account_sync_error`
  - `banking_account_sync_pending`
  - `contact_contact_new`
  - `contact_pending_review`
  - `invoicing_expense_pending`
  - `invoicing_inbox_new`
  - `invoicing_invoice_pending`
  - `invoicing_scan_pending`
  - `invoicing_scan_accountant_consumed`
  - `invoicing_purchase_count`

Deduping rule: **latest register per `client` per `date`** (ordered by `ingestion_ts DESC`).

---

## Repo structure

```text
.
├── scripts/
│   └── extract_counters_to_sheets.py
├── requirements.txt
└── .github/
    └── workflows/
        └── daily_extract_to_sheets.yml
