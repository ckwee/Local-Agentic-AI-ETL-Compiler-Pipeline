import streamlit as st
import io
import sys
import os
import duckdb
import pandas as pd
from typing import Annotated, TypedDict, List
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from langchain_ollama import ChatOllama

# =====================================================================
# 1. STREAMLIT INITIALIZATION & TRACKING
# =====================================================================
st.set_page_config(page_title="Agentic AI ETL Pipeline", layout="wide")
st.title("⚙️ Local Agentic ETL Pipeline Orchestrator")
st.caption("Powered by LangGraph, Ollama (DeepSeek), and DuckDB")

if "execution_logs" not in st.session_state:
    st.session_state.execution_logs = []
if "final_code" not in st.session_state:
    st.session_state.final_code = None

def log_to_dashboard(message: str):
    st.session_state.execution_logs.append(message)
    log_area.markdown(message)

# =====================================================================
# 2. STATE DEFINITION & AGENT NODES
# =====================================================================
class ETLState(TypedDict):
    messages: Annotated[List[dict], add_messages]
    source_file_path: str
    source_schema: str          
    target_schema: str          
    generated_code: str         
    validation_errors: str      
    iteration_count: int        

def profiling_agent(state: ETLState) -> dict:
    log_to_dashboard("🔍 **[Agent 1: Profiler]** Analyzing raw file properties...")
    file_path = state["source_file_path"]
    
    con = duckdb.connect()
    df_sample = con.execute(f"SELECT * FROM read_csv_auto('{file_path}') LIMIT 5").df()
    con.close()
    
    columns_desc = []
    for col in df_sample.columns:
        sample_vals = df_sample[col].dropna().head(2).tolist()
        columns_desc.append(f"- {col} (Sample Values: {sample_vals})")
    
    source_profile = "Raw CSV Structure:\n" + "\n".join(columns_desc)
    
    target_spec = """
    Target Table: analytics.fact_transactions
    Expected Schema:
      - customer_id (INTEGER)
      - transaction_date (DATE)
      - amount_usd (DOUBLE)
    """
    return {
        "source_schema": source_profile,
        "target_schema": target_spec,
        "iteration_count": 0
    }

def transformation_agent(state: ETLState) -> dict:
    current_iter = state['iteration_count'] + 1
    log_to_dashboard(f"🤖 **[Agent 2: Transformer]** Context sent to local `deepseek-coder-v2` instance (Attempt {current_iter})...")
    
    llm = ChatOllama(model="deepseek-coder-v2:latest", temperature=0.0)
    
    error_context = ""
    if state.get("validation_errors"):
        error_context = f"\nCRITICAL FIX NEEDED: Your last attempt threw this exact error. Correct it:\n{state['validation_errors']}"

    prompt = f"""
    You are an elite local Data Engineering Agent specializing in DuckDB pipelines. 
    Write a clean Python script using DuckDB to transform raw data into a destination table.
    
    Source File Path: '{state['source_file_path']}'
    Source Profile: 
    {state['source_schema']}
    
    Target Schema Requirements:
    {state['target_schema']}
    {error_context}
    
    CRITICAL RUNTIME RULES:
    - DO NOT call `con.close()` anywhere in the script. Leave the connection open so downstream agents can run QA checks on it.
    - DuckDB's `read_csv_auto` infers dates like 'YYYY-MM-DD' as `DATE` types natively. DO NOT call `strptime()` directly on columns that are already parsed as dates. Instead, just write `CAST(column_name AS DATE)`.
    - Clean currency symbols from numbers by casting to VARCHAR first: `replace(replace(CAST(column_name AS VARCHAR), '$', ''), ',', '')`.

    Instructions:
    1. Bind your connection to a variable exactly named `con` using: `con = duckdb.connect()`
    2. Read the source CSV using `con.execute("SELECT ... FROM read_csv_auto(...)")`.
    3. Create the schema `analytics` if it does not exist: `con.execute("CREATE SCHEMA IF NOT EXISTS analytics")`.
    4. Save the transformed records into table `analytics.fact_transactions`.
    5. Provide ONLY valid Python inside a standard markdown code block. Do not provide a conceptual explanation.
    """
    
    response = llm.invoke(prompt)
    raw_content = response.content
    
    code_lines = []
    in_code_block = False
    for line in raw_content.splitlines():
        if line.strip().startswith("```python"):
            in_code_block = True
            continue
        elif line.strip().startswith("```") and in_code_block:
            in_code_block = False
            continue
        if in_code_block:
            code_lines.append(line)
            
    clean_code = "\n".join(code_lines) if code_lines else raw_content
    return {"generated_code": clean_code, "iteration_count": current_iter}

def qa_agent(state: ETLState) -> dict:
    log_to_dashboard("🧪 **[Agent 3: QA & Executor]** Running compliance checks on generated script...")
    code = state['generated_code']
    
    old_stdout = sys.stdout
    redirected_output = sys.stdout = io.StringIO()
    
    try:
        exec_globals = {"duckdb": duckdb, "pd": pd}
        exec(code, exec_globals)
        sys.stdout = old_stdout
        
        con = exec_globals.get('con')
        if not con:
            raise ValueError("The script did not expose a valid open connection variable named 'con'.")
            
        row_count = con.execute("SELECT COUNT(*) FROM analytics.fact_transactions").fetchone()[0]
        log_to_dashboard(f"✅ **Success!** Target verified. Extracted, transformed, and loaded {row_count} records.")
        
        # We handle closing the connection safely down here once validation completes
        try:
            con.close()
        except:
            pass
            
        return {"validation_errors": ""}
        
    except Exception as e:
        sys.stdout = old_stdout
        error_msg = f"Runtime Exception: {str(e)}"
        log_to_dashboard(f"❌ **QA Error Flagged:** {error_msg}")
        return {"validation_errors": error_msg}

# =====================================================================
# 3. PIPELINE ASSEMBLY & ROUTING
# =====================================================================
def determine_next_step(state: ETLState):
    if not state["validation_errors"]:
        log_to_dashboard("🏆 **Orchestrator Notification:** Workflow completed successfully.")
        return END
    if state["iteration_count"] >= 3:
        log_to_dashboard("⚠️ **Orchestrator Notification:** Terminating loop to prevent infinite execution.")
        return END
    return "transformer"

workflow = StateGraph(ETLState)
workflow.add_node("profiler", profiling_agent)
workflow.add_node("transformer", transformation_agent)
workflow.add_node("qa_validator", qa_agent)

workflow.set_entry_point("profiler")
workflow.add_edge("profiler", "transformer")
workflow.add_edge("transformer", "qa_validator")
workflow.add_conditional_edges("qa_validator", determine_next_step)
pipeline = workflow.compile()

# =====================================================================
# 4. STREAMLIT DUAL-COLUMN LAYOUT
# =====================================================================
col_left, col_right = st.columns([1, 1])

with col_left:
    st.header("1. Input Data Configuration")
    st.write("Generating mock chaotic source data file...")
    
    mock_file = "landing_zone_source.csv"
    data = {
        "User_Serial": [9402, 9403, 9404],
        "Logged_Date_String": ["2026-07-01", "2026-07-02", "2026-07-03"],
        "Gross_Billable_Amt": ["$49.99", "$1,250.00", "300.50"]
    }
    df_preview = pd.DataFrame(data)
    df_preview.to_csv(mock_file, index=False)
    st.dataframe(df_preview, use_container_width=True)
    
    run_pipeline = st.button("🚀 Trigger Agentic ETL Loop", use_container_width=True)

with col_right:
    st.header("2. Live Agent Orchestration Monitor")
    
    log_area = st.container(border=True)
    
    with log_area:
        if st.session_state.execution_logs:
            for log in st.session_state.execution_logs:
                st.markdown(log)
        else:
            st.info("Pipeline idle. Awaiting trigger event.")

    if run_pipeline:
        st.session_state.execution_logs = []
        st.session_state.final_code = None
        
        with st.spinner("Running agents locally..."):
            initial_state = {"source_file_path": mock_file, "messages": []}
            final_output = pipeline.invoke(initial_state)
            
            if not final_output.get("validation_errors"):
                st.session_state.final_code = final_output["generated_code"]
        
        if os.path.exists(mock_file):
            os.remove(mock_file)
            
        st.rerun()  

    if st.session_state.final_code:
        st.subheader("💡 Final Auto-Generated Pipeline Script")
        st.code(st.session_state.final_code, language="python")