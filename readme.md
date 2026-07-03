# Local Agentic AI ETL Compiler Pipeline
A self-healing multi-agent ETL orchestration framework built using LangGraph, Ollama, and DuckDB.

Instead of treating the Large Language Model (LLM) as a slow, non-deterministic, row-by-row compute engine, this project implements a Code-Gen Compiler Architecture. The agents autonomously inspect incoming data schemas, dynamically write optimized native DuckDB SQL/Python transformation code, test the logic in a runtime sandbox environment, evaluate errors against dialect boundaries, and self-heal the pipeline automatically before final production execution.

# Detailed Technical Workflow
The system moves away from brittle, hardcoded transformations by treating data ingestion as a stateful agentic compiler loop managed by LangGraph.
```
                       [ Incoming Chaotic Data ]
                                   │
                                   ▼
                        ┌─────────────────────┐
                        │   1. Profiler Node  │ (Deterministic data schema extraction)
                        └──────────┬──────────┘
                                   │
                                   ▼
            ┌───────────► ┌─────────────────────┐
            │             │ 2. Transformer Node │ (DeepSeek-Coder code generation)
            │             └──────────┬──────────┘
            │                        │
    [Error Logs /                    ▼
     State Handoff]       ┌─────────────────────┐
            │             │    3. QA Executor   │ (Sandboxed evaluation & compilation)
            │             └──────────┬──────────┘
            │                        │
            │               🎨 [Pass / Fail?]
            └─────────────── False       True ───► [ Clean Target Table ]
```
### Step 1: Static Profiling (The Profiler Node)
Action: When a raw data asset lands in the ingestion zone, the system avoids sending raw data directly to the LLM context, which would waste tokens and introduce parsing non-determinism. Instead, the Profiler boots a local in-memory DuckDB connection to parse a fast metadata sample sweep:

Python
```
df_sample = con.execute(f"SELECT * FROM read_csv_auto('{file_path}') LIMIT 5").df()
```
State Impact: source_schema is populated with a clean structural map detailing data columns, inferred structural types (BIGINT, VARCHAR, DATE), and character properties.

### Step 2: Dialect-Constrained Code Synthesis (The Transformer Node)
Action: The Transformer passes the source profile, targeted schema requirements, and prior validation failure tracebacks (if looping from an error) to the local deepseek-coder-v2 instance.

Prompt Guardrails: Local open-weight models frequently fallback to standard PostgreSQL syntax. To neutralize this, the orchestration prompt injects strict System-Level Dialect Axioms:

Explicit Type Laundering: Forcing explicit string casting modifications (replace(replace(CAST(x AS VARCHAR)...)) to safely scrub dirty data text (like $1,250.00) before mathematical evaluation.

Typing Guardrails: Warning the LLM about DuckDB's native type inference (e.g., reminding the model that ISO string columns are pre-parsed as native DATE objects by read_csv_auto, making standard string conversion functions like strptime() fail).

### Step 3: Sandboxed Execution & Verification (The QA & Executor Node)
Action: The QA node loads the synthesized script string inside an isolated Python execution namespace (exec()) to prevent global variable pollution:

Python
```
exec_globals = {"duckdb": duckdb, "pd": pd}
exec(code, exec_globals)
```

Branching & Self-Healing Logic:

Catch Block: If the generated script hits a compilation or execution constraint (e.g., a DuckDB Catalog Error or Binder Error), the engine intercepts the raw stack trace via a try/except block. It writes the exact error string into validation_errors, increments iteration_count, and triggers a conditional routing edge right back to Step 2 to auto-repair the script.

Assertion Block: If execution runs without errors, the QA agent queries the execution namespace handle directly (exec_globals['con']), evaluating an internal catalog check: 
```
SELECT COUNT(*) FROM analytics.fact_transactions.
```
If rows exist and successfully conform to the destination data properties, the active connection safely closes, validation_errors clears, and the graph stops at the terminal END node.

# Tech Stack
Orchestration & State Machine: LangGraph (StateGraph framework managing loop transitions)

Local Inference Engine: Ollama (deepseek-coder-v2:latest)

Compute & Execution Engine: DuckDB (High-performance in-memory columnar processing)

Frontend UI Dashboard: Streamlit (Reactive real-time monitoring of agent states)

# Prerequisites & Installation

Install the dependencies:

Bash
```
pip install streamlit langchain-ollama langgraph duckdb pandas
```
Set up Ollama and fetch the model library:
Ensure your local Ollama instance is active, then pull down the specialized coding model:

Bash
```
ollama pull deepseek-coder-v2:latest
```

# Running the Streamlit Dashboard
To launch the real-time agent monitoring interface, execute the application from the root directory:

Bash
```
streamlit run app.py
```

# Dashboard Features

Left Panel (Data Config): Generates a mock chaotic landing zone file containing unparsed strings, unformatted dates, and raw financial symbols ($, ,) to actively stress-test the pipeline's reasoning capabilities.

Right Panel (Orchestration Monitor): Exposes the step-by-step state machine updates as agents pass context messages, report runtime exceptions, patch errors, and display the final auto-generated execution code snippet.

# Strategic Operational Guardrails
The Compiler Paradigm: Keeps infrastructure operational costs virtually flat. By isolating the LLM entirely to metadata parsing and logic scripting, the physical processing of gigabyte-to-terabyte scale matrices runs at raw C++ speeds inside DuckDB.

### State Isolation: 
The execution sandbox decouples the database lifecycle from the agent runner lifecycle by forcing explicit connection variables (con), preventing hanging resource leaks.

### Loop Deflection Guardrails: 
The state machine monitors the iteration_count. If the script fails to resolve formatting discrepancies within 3 iterations, the orchestrator triggers a fallback breakout, terminating the execution before wasting system resource cycles.

📝 License
