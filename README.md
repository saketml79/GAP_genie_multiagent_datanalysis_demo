# Supply Chain Genie Agent Workshop

A Databricks Asset Bundle (DAB) that deploys a complete supply chain analytics platform with multi-agent AI orchestration.

## Architecture

```
Supervisor Agent ("Supply Chain Control Tower")
    ├─ Demand Analysis Agent        → Genie Space (6 tables)
    ├─ Inventory Management Agent   → Genie Space (4 tables)
    ├─ Logistics Operations Agent   → Genie Space (4 tables)
    ├─ Supplier Risk Agent          → Genie Space (5 tables)
    └─ Executive Reporting Agent    → Genie Space (4 views)
```

## What Gets Created

| Component | Count | Details |
| --- | --- | --- |
| Unity Catalog | 1 catalog, 5 schemas | `GAP_Demo_{env}` with demand, inventory, logistics, supplier, reporting |
| Tables | 19 base tables | ~100K+ rows of synthetic supply chain data |
| Views | 4 reporting views | Cross-domain KPIs, regional summary, revenue trend, risk scorecard |
| Genie Spaces | 5 | Domain-specific with instructions, table descriptions, column synonyms |
| Supervisor Agent | 1 | Orchestrates all 5 Genie agents with structured response format |
| Table/Column Comments | 19 tables, 180+ columns | Semantic descriptions for Genie agent accuracy |

## Demo Story (Embedded in Data)

- **Western Region revenue drops ~30%** in the last 30 days (price reductions + higher backorder rates)
- **Western Apparel inventory critically low** (0-15 units, 12% stockout rate)
- **Western-bound shipments 42%+ late** (port congestion, carrier capacity issues)
- **Asian suppliers delayed +5-18 days** (SUP-001 TextilePro Asia worst: factory shutdowns)
- **Forecast model over-predicted Western demand by 35%**

## Deployment

### Prerequisites

- Databricks CLI installed and authenticated
- A SQL Warehouse running in the target workspace
- Permissions to create Unity Catalog objects

### Deploy to Dev

```bash
cd GAP_genie_multiagent_datanalysis_demo
databricks bundle deploy --target dev
databricks bundle run supply_chain_demo_setup --target dev
```

### Deploy to Other Environments

```bash
# Staging
databricks bundle deploy --target staging
databricks bundle run supply_chain_demo_setup --target staging

# Production
databricks bundle deploy --target prod
databricks bundle run supply_chain_demo_setup --target prod
```

### Configuration

Edit `databricks.yml` to set per-environment values:

- `catalog_name`: Unity Catalog name (default: `GAP_Demo_Dev`)
- `warehouse_id`: SQL Warehouse ID for Genie Spaces

### Teardown

Run the teardown script to remove all resources:

```bash
databricks bundle run supply_chain_demo_setup --target dev \
  --python-params '{"catalog_name": "GAP_Demo_Dev", "confirm": "YES"}'
```

Or run `src/09_teardown.py` manually as a notebook.

## File Structure

```
├── databricks.yml                              # Bundle config with dev/staging/prod targets
├── README.md                                   # This file
├── resources/
│   └── supply_chain_job.yml                    # Job resource definition (8-task pipeline)
└── src/
    ├── 01_create_catalog_schemas.py             # Create catalog + 5 schemas
    ├── 02_generate_demand_data.py               # Products, customers, sales orders, forecasts, POS, promotions
    ├── 03_generate_inventory_data.py            # Warehouses, inventory ledger, store inventory, stock movements
    ├── 04_generate_logistics_data.py            # Carriers, DCs, shipments, transit events
    ├── 05_generate_supplier_data.py             # Suppliers, POs, lead times, SLAs, procurement
    ├── 06_create_reporting_views.py             # 4 cross-domain reporting views
    ├── 07_add_all_comments.py                  # Table + column comments (180+ columns)
    ├── 08_setup_genie_supervisor.py             # 5 Genie Spaces + Supervisor Agent + examples
    ├── 09_teardown.py                           # Clean removal of all resources
    ├── improvements/                            # Iterative quality improvement demos
    │   ├── iteration_01_baseline_assessment.py   # Document baseline issues + ground truth
    │   ├── iteration_02_certified_queries.py     # Add pre-built SQL to Genie Spaces
    │   ├── iteration_03_synonyms_and_samples.py  # Add column synonyms + entity matching
    │   ├── iteration_04_benchmarks.py            # Automated validation test suite
    │   └── iteration_05_consistency_traceability.py # Cross-agent reconciliation + traceability
    └── notebooks/
        └── expected_output_reference.py          # Gold standard: correct output for demo prompt
```

## Test Prompt

After deployment, test the Supervisor Agent with:

> "Our VP of Operations just asked: Why did revenue drop in the Western Region last month, are we going to miss our quarterly service-level targets, and what immediate actions should we take? Investigate every dimension -- demand, inventory, logistics, suppliers, and overall KPIs."
