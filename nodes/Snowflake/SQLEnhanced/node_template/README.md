# DDL Utility Node Type (ID: 12)

A DDL-only node type for creating auxiliary SQL objects — UDFs, masking policies,
row access policies, sequences, or any other DDL — that support the pipeline but
aren't tables or views. Objects are qualified with `ref_no_link` so they promote
correctly across environments.

## File format

The `.sql` file needs a dummy `SELECT` at the top (the coa parser requires it),
then your DDL below a `-- @@DDL@@` marker. Separate multiple statements with
`-- @@STATEMENT@@`:

```sql
@id("<uuid>")
@nodeType("12")

SELECT 1 AS DDL_PLACEHOLDER
-- @@DDL@@
CREATE OR REPLACE FUNCTION {{ ref_no_link('SILVER', 'MY_UDF') }}(input_val STRING)
RETURNS STRING
LANGUAGE SQL
AS 'TRIM(input_val)';
-- @@STATEMENT@@
CREATE OR REPLACE MASKING POLICY {{ ref_no_link('SILVER', 'MASK_PII') }}
AS (val STRING) RETURNS STRING ->
  CASE WHEN CURRENT_ROLE() IN ('SYSADMIN') THEN val ELSE '***MASKED***' END;
-- @@STATEMENT@@
CREATE OR REPLACE SEQUENCE {{ ref_no_link('SILVER', 'SEQ_SURROGATE') }};
```

### Why the dummy SELECT?

The coa `.sql` file parser extracts column metadata from a SELECT statement. DDL
statements like `CREATE FUNCTION` can't be parsed this way. The `SELECT 1 AS
DDL_PLACEHOLDER` satisfies the parser; the template ignores it and executes
everything below `-- @@DDL@@`.

### Why `-- @@STATEMENT@@` instead of `---`?

The `---` delimiter is interpreted as a YAML document separator by the coa file
loader, which causes it to lose the `@id` annotation. `-- @@STATEMENT@@` is a
safe SQL comment that the template splits on.

## Building

DDL nodes only need `coa create` — there is no run step:

```bash
coa create --include "{ DDL_TRIM_UDFS }"
```

## Applying policies

Masking policies and row access policies are created by the DDL node, then applied
via ALTER statements. You can include the ALTER in the same DDL node (after the
CREATE), or use `postSQL` on consuming nodes.

**In the DDL node (apply immediately after creation):**
```sql
-- @@STATEMENT@@
ALTER TABLE {{ ref_no_link('GOLD', 'FCT_TRADE') }}
  ALTER COLUMN COUNTERPARTY SET MASKING POLICY {{ ref_no_link('SILVER', 'MASK_PII') }};
```

**Via postSQL on a consuming node (applied after each run):**
Set `postSQL` config in the node type definition or via `coa serve`.

## Calling DDL objects from pipeline nodes

There are three ways to qualify DDL object calls. The approach differs between
V2 SQL nodes and V1 YAML nodes.

### V2 SQL nodes — CTE pattern required

The coa V2 column parser can't handle qualified function calls like
`DB.SCHEMA.FN(col)` in a SELECT. Wrap them in a CTE so the outer SELECT has
bare column names.

**Option A: `loc_fn` macro** (cleanest)
```sql
WITH prepared AS (
    SELECT
        {{ loc_fn("SILVER", "MY_UDF(TRADE_ID)") }}   AS TRADE_ID,
        TRADE_DATE::DATE                              AS TRADE_DATE
    FROM {{ ref('SRC_TRADES', 'RAW_TRADES') }}
)
SELECT TRADE_ID, TRADE_DATE FROM prepared
```

**Option B: Parameters** (environment-specific via config)
```sql
WITH prepared AS (
    SELECT
        {{ parameters.ddl_database ~ "." ~ parameters.ddl_schema ~ ".MY_UDF(TRADE_ID)" }}   AS TRADE_ID
    FROM {{ ref('SRC_TRADES', 'RAW_TRADES') }}
)
SELECT TRADE_ID FROM prepared
```
Requires `parameters={"ddl_database": "MY_DB", "ddl_schema": "MY_SCHEMA"}` in
`~/.coa/config` or passed via `--parameters`.

**Option C: `ref_no_link` shorthand** (no macros/config needed)
```sql
WITH prepared AS (
    SELECT
        {{ ref_no_link("SILVER", "MY_UDF") | replace('"', '') ~ "(TRADE_ID)" }}   AS TRADE_ID
    FROM {{ ref('SRC_TRADES', 'RAW_TRADES') }}
)
SELECT TRADE_ID FROM prepared
```

### V1 YAML nodes — direct transform, no CTE needed

V1 YAML nodes store transforms as raw strings. Qualified function calls work
directly in the `transform:` field:

```yaml
# Using loc_fn macro:
transform: "{{ loc_fn('SILVER', 'MY_UDF(\"RAW_TRADES\".\"TRADE_ID\")') }}"

# Using parameters:
transform: "{{ parameters.ddl_database }}.{{ parameters.ddl_schema }}.MY_UDF(\"RAW_TRADES\".\"TRADE_ID\")"

# Hardcoded (not promotable):
transform: "MY_DATABASE.SILVER.MY_UDF(\"RAW_TRADES\".\"TRADE_ID\")"
```

### Sequences in transforms

Sequences are called as `DB.SCHEMA.SEQ_NAME.NEXTVAL`. Same CTE pattern for V2,
direct transform for V1:

```sql
-- V2 SQL (in CTE):
{{ loc_fn("SILVER", "SEQ_TRADE_SK.NEXTVAL") }}   AS TRADE_SK
```

```yaml
# V1 YAML:
transform: "{{ loc_fn('SILVER', 'SEQ_TRADE_SK.NEXTVAL') }}"
```

### Available macros

Defined in `macros/location_helpers-2.yml`:

| Macro | Example | Resolves to |
|---|---|---|
| `loc_db("SILVER")` | Database name | `MY_DATABASE` |
| `loc_schema("SILVER")` | Schema name | `SILVER` |
| `loc_path("SILVER")` | DB.SCHEMA | `MY_DATABASE.SILVER` |
| `loc_fn("SILVER", "FN(col)")` | Qualified call | `MY_DATABASE.SILVER.FN(col)` |

## Supported DDL types

| Type | CREATE syntax | How it's used |
|---|---|---|
| UDF | `CREATE OR REPLACE FUNCTION` | Called in column transforms |
| Masking policy | `CREATE OR REPLACE MASKING POLICY` | Applied via `ALTER TABLE ... SET MASKING POLICY` |
| Row access policy | `CREATE OR REPLACE ROW ACCESS POLICY` | Applied via `ALTER TABLE ... ADD ROW ACCESS POLICY` |
| Sequence | `CREATE OR REPLACE SEQUENCE` | Called as `SEQ.NEXTVAL` in transforms |
| File format | `CREATE OR REPLACE FILE FORMAT` | Referenced in `COPY INTO` or external tables |
| Tag | `CREATE OR REPLACE TAG` | Applied via `ALTER TABLE ... SET TAG` |

## Template behaviour

The `create.sql.j2` template:

1. Reads the raw file content via `sources[0].customSQL`
2. Finds the `-- @@DDL@@` marker
3. Splits everything after it on `-- @@STATEMENT@@`
4. Executes each block as a separate Coalesce stage

The `run.sql.j2` is a no-op placeholder — DDL nodes have no DML.
