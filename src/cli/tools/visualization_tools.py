#!/usr/bin/env python3
"""
LLM-Powered Visualization function tools for the Agentic Query CLI
"""

import sys
import os
import pandas as pd
import json
import tempfile
import webbrowser
from typing import Optional, List, Dict, Any

# Add the project root to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from utils import llm_util


def visualize_data_impl(agent_context, user_request: str = "create a chart") -> str:
    """LLM-powered visualization generator based on user request and data analysis"""
    
    print(f"DEBUG VIZ: Starting visualization with request: {user_request}")
    
    if not agent_context.last_query_results:
        print("DEBUG VIZ: No query results available")
        return "❌ No query results available for visualization. Please run a query first."
    
    try:
        print(f"DEBUG VIZ: Found {len(agent_context.last_query_results)} rows of data")
        
        # Convert results to DataFrame for analysis
        df = pd.DataFrame(agent_context.last_query_results)
        print(f"DEBUG VIZ: DataFrame shape: {df.shape}")
        print(f"DEBUG VIZ: DataFrame columns: {list(df.columns)}")
        print(f"DEBUG VIZ: DataFrame dtypes:\n{df.dtypes}")
        
        if df.empty:
            print("DEBUG VIZ: DataFrame is empty")
            return "❌ No data available to visualize."
        
        # Get data summary for LLM analysis
        print("DEBUG VIZ: Analyzing data structure...")
        data_summary = _analyze_data_structure(df)
        print(f"DEBUG VIZ: Data summary created with {len(data_summary['columns'])} columns")
        
        # Use LLM to analyze data and generate visualization plan
        print("DEBUG VIZ: Calling LLM for visualization plan...")
        viz_plan = _get_llm_visualization_plan(data_summary, user_request, agent_context.last_query_sql)
        
        print(f"DEBUG VIZ: LLM plan status: {viz_plan.get('status', 'unknown')}")
        if viz_plan["status"] == "error":
            print(f"DEBUG VIZ: LLM planning error: {viz_plan['error']}")
            return f"❌ Visualization planning failed: {viz_plan['error']}"
        
        print(f"DEBUG VIZ: Chart type: {viz_plan.get('chart_type', 'unknown')}")
        print(f"DEBUG VIZ: Generated code length: {len(viz_plan.get('chart_code', ''))}")
        
        # Generate and execute the chart code using LLM
        print("DEBUG VIZ: Executing chart code...")
        chart_result = _execute_llm_chart_code(df, viz_plan["chart_code"], viz_plan["explanation"])
        
        print(f"DEBUG VIZ: Chart execution result length: {len(chart_result)}")
        return chart_result
        
    except Exception as e:
        print(f"DEBUG VIZ: Exception occurred: {str(e)}")
        import traceback
        print(f"DEBUG VIZ: Traceback: {traceback.format_exc()}")
        return f"❌ Error creating LLM-powered visualization: {str(e)}"


def get_visualization_suggestions_impl(agent_context) -> str:
    """LLM-powered visualization suggestions based on data analysis"""
    
    if not agent_context.last_query_results:
        return "❌ No query results available. Please run a query first."
    
    try:
        df = pd.DataFrame(agent_context.last_query_results)
        
        if df.empty:
            return "❌ No data available for visualization suggestions."
        
        # Get comprehensive data analysis
        data_summary = _analyze_data_structure(df)
        
        # Use LLM to generate intelligent suggestions
        suggestions = _get_llm_suggestions(data_summary, agent_context.last_query_sql)
        
        return suggestions
        
    except Exception as e:
        return f"❌ Error generating LLM suggestions: {str(e)}"


def _analyze_data_structure(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze DataFrame structure for LLM input"""
    
    # Convert sample data to JSON-safe format
    sample_data = []
    for record in df.head(3).to_dict('records'):
        safe_record = {}
        for k, v in record.items():
            if pd.isna(v):
                safe_record[k] = None
            elif isinstance(v, pd.Timestamp):
                safe_record[k] = v.isoformat()
            elif hasattr(v, 'item'):  # numpy types
                safe_record[k] = v.item()
            else:
                safe_record[k] = str(v) if v is not None else None
        sample_data.append(safe_record)
    
    analysis = {
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": [],
        "sample_data": sample_data,
        "data_types": {}
    }
    
    for col in df.columns:
        col_info = {
            "name": col,
            "dtype": str(df[col].dtype),
            "null_count": int(df[col].isnull().sum()),
            "unique_count": int(df[col].nunique())
        }
        
        # Add specific analysis based on data type
        if df[col].dtype in ['int64', 'float64']:
            col_info.update({
                "min": float(df[col].min()) if pd.notna(df[col].min()) else None,
                "max": float(df[col].max()) if pd.notna(df[col].max()) else None,
                "mean": float(df[col].mean()) if pd.notna(df[col].mean()) else None
            })
        elif df[col].dtype == 'object':
            # Convert pandas value counts to JSON-safe format
            top_values = df[col].value_counts().head(5)
            col_info["top_values"] = {str(k): int(v) for k, v in top_values.items()}
        
        analysis["columns"].append(col_info)
        analysis["data_types"][col] = str(df[col].dtype)
    
    return analysis


def _get_llm_visualization_plan(data_summary: Dict, user_request: str, sql_query: Optional[str]) -> Dict:
    """Use LLM to create a visualization plan"""
    
    print("DEBUG VIZ: Starting LLM visualization plan generation")
    
    system_prompt = """You are a data visualization expert. Analyze the provided data structure and user request to create the best possible chart.

Your tasks:
1. Analyze the data structure and understand what story the data tells
2. Recommend the most appropriate chart type based on the data and user request
3. Generate Python code using plotly to create an interactive chart
4. Provide a clear explanation of why this visualization is optimal

IMPORTANT RULES:
- Always use plotly (px or go) for interactive charts
- The DataFrame variable is called 'df' 
- Return valid Python code that can be executed directly
- Include proper error handling
- Make charts visually appealing with titles, labels, and colors
- Consider the data types and relationships when choosing chart types

Return your response as JSON with these keys:
- chart_type: string (e.g., "bar", "line", "scatter", "pie", "histogram", "box")
- chart_code: string (complete Python code to generate the chart)
- explanation: string (why this chart type and configuration is best)
- title: string (descriptive chart title)
"""

    user_prompt = f"""
Data Structure Analysis:
{json.dumps(data_summary, indent=2)}

Original SQL Query: {sql_query or "Not available"}

User Request: {user_request}

Please analyze this data and create the best visualization plan. Consider:
- What insights can be gained from this data?
- What chart type best represents the relationships?
- How can we make the visualization most informative and engaging?

Generate Python plotly code that will create an excellent interactive chart.
"""

    try:
        print("DEBUG VIZ: Sending request to LLM...")
        print(f"DEBUG VIZ: User prompt length: {len(user_prompt)}")
        
        response = llm_util.call_response_api(llm_util.llm_model, system_prompt, user_prompt)
        result_text = response.choices[0].message.content
        
        print(f"DEBUG VIZ: LLM response length: {len(result_text)}")
        print(f"DEBUG VIZ: LLM response preview: {result_text[:200]}...")
        
        # Try to parse JSON response
        try:
            original_text = result_text
            if result_text.startswith("```json"):
                result_text = result_text[7:-3]
            elif result_text.startswith("```"):
                result_text = result_text[3:-3]
            
            print(f"DEBUG VIZ: Attempting JSON parse on: {result_text[:100]}...")
            viz_plan = json.loads(result_text)
            viz_plan["status"] = "success"
            print("DEBUG VIZ: JSON parsing successful")
            return viz_plan
            
        except json.JSONDecodeError as je:
            print(f"DEBUG VIZ: JSON parsing failed: {str(je)}")
            print(f"DEBUG VIZ: Attempting code extraction from: {original_text[:200]}...")
            
            # If JSON parsing fails, extract code manually
            return {
                "status": "success",
                "chart_type": "auto",
                "chart_code": original_text,
                "explanation": "LLM generated visualization code",
                "title": "Data Visualization"
            }
            
    except Exception as e:
        print(f"DEBUG VIZ: LLM call failed: {str(e)}")
        import traceback
        print(f"DEBUG VIZ: LLM traceback: {traceback.format_exc()}")
        return {
            "status": "error",
            "error": f"LLM visualization planning failed: {str(e)}"
        }


def _get_llm_suggestions(data_summary: Dict, sql_query: Optional[str]) -> str:
    """Use LLM to generate intelligent visualization suggestions"""
    
    system_prompt = """You are a data visualization consultant. Analyze the provided data and suggest the best visualization options.

Provide practical, actionable suggestions that consider:
- Data types and relationships
- Data volume and distribution  
- Business insights that can be revealed
- Different chart types for different purposes

Format your response as a helpful guide with specific recommendations and example commands.
"""

    user_prompt = f"""
Data Analysis:
{json.dumps(data_summary, indent=2)}

Original SQL Query: {sql_query or "Not available"}

Please provide intelligent visualization suggestions for this dataset. Include:
1. Overview of the data characteristics
2. Top 3-5 recommended chart types with explanations
3. Specific insights each chart type would reveal
4. Example commands the user can use (like "create a bar chart showing sales by region")

Make the suggestions practical and insightful.
"""

    try:
        response = llm_util.call_response_api(llm_util.llm_model, system_prompt, user_prompt)
        return f"🤖 **LLM Analysis & Suggestions:**\n\n{response.choices[0].message.content}"
        
    except Exception as e:
        return f"❌ Error getting LLM suggestions: {str(e)}"


def _execute_llm_chart_code(df: pd.DataFrame, chart_code: str, explanation: str) -> str:
    """Execute the LLM-generated chart code safely"""
    
    print("DEBUG VIZ: Starting chart code execution")
    print(f"DEBUG VIZ: Chart code preview: {chart_code[:200]}...")
    
    try:
        # Import required libraries for code execution
        import plotly.express as px
        import plotly.graph_objects as go
        import numpy as np
        
        print("DEBUG VIZ: Libraries imported successfully")
        
        # Create safe execution environment
        safe_globals = {
            'df': df,
            'px': px,
            'go': go,
            'np': np,
            'pd': pd
        }
        
        print("DEBUG VIZ: Safe execution environment created")
        print(f"DEBUG VIZ: DataFrame in environment shape: {safe_globals['df'].shape}")
        
        # Execute the LLM-generated code
        print("DEBUG VIZ: Executing chart code...")
        exec(chart_code, safe_globals)
        print("DEBUG VIZ: Code execution completed")
        
        # Look for the figure object
        fig = None
        available_vars = [k for k in safe_globals.keys() if not k.startswith('_')]
        print(f"DEBUG VIZ: Available variables after execution: {available_vars}")
        
        for var_name in ['fig', 'figure', 'chart']:
            if var_name in safe_globals:
                fig = safe_globals[var_name]
                print(f"DEBUG VIZ: Found figure object in variable: {var_name}")
                break
        
        if fig is None:
            print("DEBUG VIZ: No figure object found with standard names")
            # Try to find any plotly figure objects
            for var_name, var_value in safe_globals.items():
                if hasattr(var_value, 'write_html'):
                    fig = var_value
                    print(f"DEBUG VIZ: Found plotly figure in variable: {var_name}")
                    break
        
        if fig is None:
            print("DEBUG VIZ: No figure object found at all")
            return f"❌ No figure object found in generated code. Code executed but no chart was created.\n\nAvailable variables: {available_vars}\n\nGenerated code:\n```python\n{chart_code}\n```"
        
        # Save and display the chart
        temp_dir = tempfile.gettempdir()
        chart_path = os.path.join(temp_dir, "nl2sql_llm_chart.html")
        
        print(f"DEBUG VIZ: Saving chart to: {chart_path}")
        fig.write_html(chart_path)
        print("DEBUG VIZ: Chart saved successfully")
        
        # Try to open in browser
        success_msg = f"✅ **LLM-Generated Chart Created Successfully!**\n\n"
        success_msg += f"📊 **Explanation**: {explanation}\n\n"
        success_msg += f"📁 **Chart saved to**: {chart_path}\n"
        
        try:
            print("DEBUG VIZ: Attempting to open in browser...")
            webbrowser.open(f"file://{chart_path}")
            success_msg += "🌐 **Chart opened in browser**"
            print("DEBUG VIZ: Browser opening successful")
        except Exception as browser_error:
            print(f"DEBUG VIZ: Browser opening failed: {str(browser_error)}")
            success_msg += "💡 **Tip**: Open the HTML file in your browser to view the interactive chart"
        
        return success_msg
        
    except Exception as e:
        print(f"DEBUG VIZ: Chart execution failed: {str(e)}")
        import traceback
        print(f"DEBUG VIZ: Execution traceback: {traceback.format_exc()}")
        return f"❌ Error executing LLM chart code: {str(e)}\n\nGenerated code:\n```python\n{chart_code}\n```"