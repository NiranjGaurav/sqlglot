# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SQLGlot is a comprehensive SQL parser, transpiler, optimizer, and engine written in Python. It supports 31+ SQL dialects and provides tools for SQL formatting, translation, analysis, and execution.

## Key Architecture Components

### Core Modules
- **Parser** (`sqlglot/parser.py`): Converts SQL strings into Abstract Syntax Trees (AST)
- **Generator** (`sqlglot/generator.py`): Converts AST back to SQL strings 
- **Expressions** (`sqlglot/expressions.py`): Defines all AST node types
- **Dialects** (`sqlglot/dialects/`): SQL dialect-specific parsing and generation logic
- **Optimizer** (`sqlglot/optimizer/`): Query optimization rules and transformations
- **Executor** (`sqlglot/executor/`): Python-based SQL execution engine

### AST-Based Design
SQLGlot uses an Abstract Syntax Tree approach where SQL is parsed into expression nodes that can be:
- Traversed using `.find()` and `.find_all()` methods
- Transformed using `.transform()` method  
- Modified in-place using `.set()` and direct property assignment
- Converted back to SQL using `.sql()` method

### Dialect System
Each dialect in `sqlglot/dialects/` inherits from the base `Dialect` class and can override:
- `Tokenizer`: Controls lexical analysis and keywords
- `Parser`: Handles dialect-specific parsing rules  
- `Generator`: Manages SQL generation and formatting

## Development Commands

### Installation and Setup
```bash
make install          # Install package in development mode
make install-dev      # Install with development dependencies + Rust tokenizer
make install-dev-core # Install only Python dev dependencies
```

### Testing
```bash
make test       # Run full test suite (Python tokenizer)
make test-rs    # Run full test suite (Rust tokenizer)  
make unit       # Run unit tests only (Python tokenizer)
make unit-rs    # Run unit tests only (Rust tokenizer)
```

### Code Quality
```bash
make style      # Run linting and formatting checks
make check      # Run style + all tests
```

### Benchmarking
```bash
make bench          # Run parsing benchmarks
make bench-optimize # Run optimization benchmarks
```

### Documentation
```bash
make docs       # Generate API documentation
make docs-serve # Serve docs locally on port 8002
```

## Testing Guidelines

- Unit tests are in `tests/` directory organized by module
- Dialect-specific tests are in `tests/dialects/`
- Integration tests use `SKIP_INTEGRATION=1` environment variable to skip
- Test fixtures for optimizations are in `tests/fixtures/optimizer/`
- Use `python -m unittest tests.test_module.TestClass.test_method` for specific tests

## Code Conventions

### Expression Handling
- Use `parse_one()` for single statements, `parse()` for multiple
- Access AST nodes via properties: `select.expressions`, `table.name`, etc.
- Use `find_all(exp.Column)` to locate all column references
- Transform trees with `expression.transform(lambda node: new_node)`

### Dialect Development
- New dialects should inherit from `Dialect` class
- Override `Tokenizer.KEYWORDS` for dialect-specific keywords
- Override `Generator.TRANSFORMS` for custom SQL generation
- Use `TYPE_MAPPING` to map between SQLGlot and dialect data types

### Error Handling
- Use `ErrorLevel.RAISE` to fail on unsupported features
- Use `ErrorLevel.WARN` for best-effort transpilation with warnings
- Parse errors include line/column information for debugging

## Optimizer System

The optimizer applies transformation rules in sequence:
1. **qualify**: Resolve table/column references and add explicit qualifiers
2. **annotate_types**: Infer and annotate expression types
3. **normalize**: Standardize expression forms
4. **eliminate_subqueries**: Remove unnecessary subqueries
5. **pushdown_predicates**: Move WHERE conditions closer to data sources
6. **optimize_joins**: Reorder and optimize JOIN operations

## Performance Notes

- Rust tokenizer (`sqlglotrs`) provides ~2x parsing speedup when available
- Use `identify=True` in transpile for automatic identifier quoting
- Schema information improves optimization but adds overhead
- The executor is for testing/prototyping, not production workloads

## Contributing Guidelines

- Follow Conventional Commits for PR titles
- Keep PRs focused on single, well-defined changes
- Add tests for non-trivial functionality changes
- Update docstrings for API changes
- Ensure `make check` passes before submitting

## Single Test Execution

### Running specific test methods
```bash
python -m unittest tests.test_module.TestClass.test_method
```

### Examples of running specific tests
```bash
# Run all tests for a specific dialect
python -m unittest tests.dialects.test_snowflake

# Run a specific test class
python -m unittest tests.test_optimizer.TestOptimizer

# Run a specific test method
python -m unittest tests.test_transpile.TestTranspile.test_time

# Run tests with different tokenizers
SQLGLOTRS_TOKENIZER=0 python -m unittest tests.dialects.test_postgres
RUST_BACKTRACE=1 python -m unittest tests.dialects.test_postgres
```

## Additional Development Info

### Key API Entry Points
- `sqlglot.transpile()`: High-level function for SQL conversion between dialects
- `sqlglot.parse_one()`: Parse single SQL statement into AST
- `sqlglot.parse()`: Parse multiple SQL statements
- `Expression.sql()`: Convert AST back to SQL string
- `Expression.transform()`: Apply transformations to AST nodes

### Custom Features in This Codebase

This repository extends SQLGlot with several enterprise-grade features:

#### API Layer (`apis/` and `converter_api.py`)
- **FastAPI-based converter service**: Main SQL transpilation API with advanced features
- **Multiple endpoints**: `/convert-query`, `/statistics`, `/guardrail`, `/transpile-guardrail`, `/guardstats`
- **Feature flag support**: JSON-based feature configuration via `feature_flags` parameter
- **Two-phase qualification**: Advanced table qualification with `USE_TWO_PHASE_QUALIFICATION_SCHEME`
- **Structured error handling**: Comprehensive logging and error reporting
- **Performance optimization**: Multi-worker deployment with auto-scaling based on CPU cores

#### Guardrail System (`guardrail/`)
- **SQL validation engine**: Rule-based query validation before execution
- **Schema service integration**: Real-time schema validation via Thrift services
- **Table-specific rules**: Custom validation rules per table/schema
- **Security enforcement**: Prevents unauthorized data access patterns
- **Integration with storage services**: Validates against actual table metadata

#### Frontend Tools
- **Streamlit interface** (`frontend_with_dbTable.py`): Web UI for SQL conversion and table extraction
- **Batch processing**: CSV-based query conversion with parallel processing
- **Table analysis**: Automatic extraction of database and table names from queries

#### Custom Dialect Support
- **E6 dialect** (`sqlglot/dialects/e6.py`): Custom SQL dialect for internal systems
- **Enhanced transpilation**: Special handling for STRUCT types and custom functions

### Environment Variables
- `SQLGLOTRS_TOKENIZER=0`: Force Python tokenizer (slower but more compatible)
- `RUST_BACKTRACE=1`: Enable Rust tokenizer with debugging
- `SKIP_INTEGRATION=1`: Skip integration tests during unit testing
- `ENABLE_GUARDRAIL`: Enable/disable guardrail validation service
- `STORAGE_ENGINE_URL`: Hostname for storage service integration
- `STORAGE_ENGINE_PORT`: Port for storage service integration
- `UVICORN_WORKERS`: Override auto-calculated worker count for API service

### Development Workflow

#### Running the API Services
```bash
# Start main converter API (auto-scales workers)
python converter_api.py

# Start modular API with specific components
python apis/app.py

# Start Streamlit frontend
streamlit run frontend_with_dbTable.py
```

#### Type Checking and Linting
The codebase uses pre-commit hooks for code quality:
```bash
make style              # Run pre-commit hooks (linting, formatting)
pre-commit run --all-files  # Run all pre-commit checks manually
```