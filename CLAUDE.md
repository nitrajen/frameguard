# dfguard — Claude Instructions

## Documentation standards

These rules apply to every RST and Markdown file in `docs/` and to `README.md`.
Check for violations before finalising any documentation change.

### Style

- **No em dashes.** Never write `—`. Rewrite the sentence using a comma, colon,
  or two sentences instead.
- **No hyperbole.** No "blazing fast", "effortlessly", "you spend three hours
  debugging", dramatic storytelling, or LLM-sounding superlatives.
- **Library-agnostic language** in all general sections. Use "transform function",
  "pipeline stage", "column". Not `withColumn`, "stage 2/5", etc.

### Content rules

- **Pandera differentiation** must appear near the top of the homepage, quickstart,
  and README. Key message: "Unlike pandera, which introduces its own type system,
  dfguard uses the types your library already ships with."
- **`arm()` must be shown** in the homepage example and quickstart. It is the
  primary adoption path for existing codebases, not `@enforce`.
- **Quickstart must be generic.** No Kedro, Airflow, or other framework references
  inline. Framework guides live in `airflow.rst` and `kedro.rst` only.
- **PyArrow with pandas must be called out explicitly** in the pandas sections of
  both `quickstart.rst` and `types.rst`. Key message: `pd.ArrowDtype` gives pandas
  full nested-type enforcement, the same as PySpark and Polars.
- **Types page must be comprehensive** and accurate from official library sources.
  Include links. Note that scalar coverage is verified at runtime via dynamic
  subclass discovery.

### Structure

- **API reference is unified**, not split by backend. Single `api/index.rst` with
  tab-sets per section. Order: Enforcement > Schemas > Dataset > History > Exceptions.
- **Toctree order**: `quickstart > types > api/index > pipelines > airflow > kedro`.
- **Tab-sets use `:sync:`** so the backend selection persists across pages.

## Code standards

- Functional style, concise. No unnecessary verbosity.
- Tests should be minimal while keeping coverage high. Merge shallow
  single-assertion tests into one test where the intent is still clear.
- No backwards-compatibility shims, unused variables, or removed-code comments.
