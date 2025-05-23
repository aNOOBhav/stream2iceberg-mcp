import os
import getpass
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional
from langchain_anthropic import ChatAnthropic
import pandas as pd
import s3fs


# === Env Setup ===
os.environ["no_proxy"] = "api.anthropic.com"

def _set_env(var: str):
    if not os.environ.get(var):
        os.environ[var] = getpass.getpass(f"{var}: ")

_set_env("ANTHROPIC_API_KEY")

llm = ChatAnthropic(model="claude-3-5-sonnet-20240620")

# === Connect to MinIO and Read Parquet ===
fs = s3fs.S3FileSystem(
    key="admin",
    secret="password",
    client_kwargs={"endpoint_url": "http://127.0.0.1:9000"}
)

parquet_path = "warehouse/stock_iceberg_table_dump"

def load_parquet() -> pd.DataFrame:
    files = fs.ls(parquet_path)
    all_dfs = []
    for f in files:
        with fs.open(f, "rb") as fobj:
            df = pd.read_parquet(fobj)
            all_dfs.append(df)
    return pd.concat(all_dfs, ignore_index=True)

# === MCP FastAPI Backend ===
app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3007"],  # include all dev ports
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

data = load_parquet()
data['trade_date'] = pd.to_datetime(data['trade_date']).dt.strftime('%Y-%m-%d')

print(f"Loaded {len(data)} rows.")

class ToolInput(BaseModel):
    ticker: Optional[str] = None
    date: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None


class MCPRequest(BaseModel):
    tool_name: str
    input: ToolInput
    messages: List[Dict[str, str]]

@app.post("/v1/mcp")
def mcp_handler(req: MCPRequest):
    try:
        tool = req.tool_name
        args = req.input

        if tool == "query_stock_data":
            df = data[
                (data["ticker"] == args.ticker) &
                (data["trade_date"] == args.date)
            ]
            return {"result": df.to_dict(orient="records")}

        elif tool == "get_price_range":
            df = data[
                (data["ticker"] == args.ticker) &
                (data["trade_date"] >= args.start_date) &
                (data["trade_date"] <= args.end_date)
            ][["trade_date", "open_price", "close_price"]]
            return {"result": df.to_dict(orient="records")}

        elif tool == "summarize_volume":
            df = data[
                (data["ticker"] == args.ticker) &
                (data["trade_date"] >= args.start_date) &
                (data["trade_date"] <= args.end_date)
            ]
            total_volume = df["volume"].sum()
            return {
                "result": [{
                    "ticker": args.ticker,
                    "total_volume": int(total_volume)
                }]
            }

        elif tool == "ask_question_on_stock_data":
            system_prompt = (
                "You are a stock analyst assistant. Based on the user's question, extract:\n"
                "- ticker: stock ticker symbol (e.g., 'MSFT')\n"
                "- dates: list of dates mentioned in the question (in YYYY-MM-DD format)\n"
                "- metric: one of [open_price, close_price, pct_change, volume]\n"
                "- action: 'compare' if the question asks about difference or comparison\n"
                "Return only a valid JSON object. If multiple dates are provided, ensure they are extracted in chronological order."
            )

            context_prompt = (
                "When extracting dates from the question:\n"
                "1. Extract exactly the dates mentioned in the question\n"
                "2. If the question asks about a specific date range (e.g., 'from X to Y'), extract both dates\n"
                "3. If the question asks about comparison between two dates (e.g., 'difference between X and Y'), extract both dates\n"
                "4. Do NOT infer or add additional dates that are not explicitly mentioned in the question\n"
                "5. Return dates in chronological order\n"
                "6. If no dates are mentioned, return an empty dates array\n"
            )

            combined_prompt = f"{system_prompt}\n\n{context_prompt}\n\nUser question: {req.messages[-1]['content']}"

            response = llm.invoke([
                {"role": "system", "content": combined_prompt},
            ])

            import json
            try:
                tool_input = json.loads(response.content)
                ticker = tool_input.get("ticker")
                metric = tool_input.get("metric")
                dates = tool_input.get("dates", [])
                action = tool_input.get("action")

                if not ticker or not metric:
                    return {"error": "Missing 'ticker' or 'metric' in parsed input."}

                # Filter data by ticker
                df_filtered = data[data["ticker"] == ticker]

                # If specific dates are provided, filter by those dates
                if dates:
                    # Convert dates to datetime objects
                    date_objects = [pd.to_datetime(d) for d in dates]
                    # Sort dates to ensure chronological order
                    date_objects.sort()
                    # Convert back to string format
                    dates = [d.strftime('%Y-%m-%d') for d in date_objects]
                    
                    # Filter the data for exact matches
                    df_filtered = df_filtered[df_filtered["trade_date"].isin(dates)]
                    
                    # If we have exactly 2 dates and this is a comparison query
                    if len(dates) == 2 and action == "compare":
                        # Get the values for both dates
                        first_date = dates[0]
                        last_date = dates[1]
                        
                        first_value = df_filtered[df_filtered["trade_date"] == first_date][metric].iloc[0]
                        last_value = df_filtered[df_filtered["trade_date"] == last_date][metric].iloc[0]
                        
                        difference = last_value - first_value
                        percentage_change = (difference / first_value) * 100
                        
                        summary = f"The {metric} for {ticker} changed from {first_value:.2f} on {first_date} to {last_value:.2f} on {last_date}. "
                        summary += f"The change was {difference:.2f} ({percentage_change:.2f}%)."
                        
                        return {
                            "query": tool_input,
                            "result": {
                                "first_date": first_date,
                                "first_value": first_value,
                                "last_date": last_date,
                                "last_value": last_value,
                                "difference": difference,
                                "percentage_change": percentage_change
                            },
                            "summary": summary
                        }
                
                # If no specific dates or not a comparison query, get the latest data
                if df_filtered.empty:
                    return {"result": "No data found for the provided filters."}

                if metric not in df_filtered.columns:
                    return {"error": f"Unknown metric '{metric}'."}

                summary_data = df_filtered[["trade_date", metric]].to_dict(orient="records")

                # If this is a comparison query but we don't have exactly 2 dates
                if action == "compare" and len(summary_data) != 2:
                    return {"error": "Comparison requires exactly 2 dates."}

                return {
                    "query": tool_input,
                    "result": summary_data,
                    "summary": summarize_with_claude(req.messages[-1]["content"], summary_data)
                }

        return {"error": f"Unknown tool: {tool}"}

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": str(e)}

# === Run with: python mcp_server.py ===
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)