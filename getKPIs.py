import azure.functions as func
import logging
import os
import pyodbc

getKPIs = func.Blueprint()

SQLConnString = os.environ['SQL_Connection_String']

@getKPIs.route(route="get_kpis_http_trigger", auth_level=func.AuthLevel.ANONYMOUS)
def get_kpis_http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
        conn = pyodbc.connect(SQLConnString)
        cursor = conn.cursor()

        # Table should be created ahead of time in production app.
        cursor.execute("""
            SELECT * FROM dbo.receipts
            WHERE transaction_date
        """,
        (parsed_data['date'], parsed_data['amount'], parsed_data['vat']))

        conn.commit()
    except Exception as e:
        logging.warning(f'Error in accessing SQL data {e}')
        return func.HttpResponse(
                "Error in accessing DB",
                status_code=400
        )

    return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200
    )