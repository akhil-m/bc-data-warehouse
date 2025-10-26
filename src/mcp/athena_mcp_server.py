from fastmcp import FastMCP
import boto3
import time
from .retry import generate_retry_delays

mcp = FastMCP("StatsCan")
athena = boto3.client('athena', region_name='us-east-2')

# Imperative Shell
@mcp.tool
def query(sql: str) -> dict:
    """Query StatsCan data warehouse with SQL"""
    exec_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={'Database': 'statscan'},
        ResultConfiguration={'OutputLocation': 's3://athena-queries-akhil1710/'}
    )['QueryExecutionId']

    delays = generate_retry_delays(max_retries=20)

    for i, delay in enumerate(delays):
        status = athena.get_query_execution(QueryExecutionId=exec_id)
        state = status['QueryExecution']['Status']['State']

        if state not in ['QUEUED', 'RUNNING']:
            break

        if i < len(delays) - 1:
            time.sleep(delay)
    else:
        raise TimeoutError(f"Query timeout after {sum(delays):.0f}s")

    if state != 'SUCCEEDED':
        raise Exception(status['QueryExecution']['Status'].get('StateChangeReason', state))

    results = athena.get_query_results(QueryExecutionId=exec_id)
    rows = results['ResultSet']['Rows']

    return {
        'columns': [c['VarCharValue'] for c in rows[0]['Data']],
        'data': [[c.get('VarCharValue', '') for c in r['Data']] for r in rows[1:]]
    }

if __name__ == "__main__":
    mcp.run(transport="http", host="0.0.0.0", port=8001)
