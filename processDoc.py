import azure.functions as func
import logging
import base64
import requests
import os
import time
import pyodbc

processDoc = func.Blueprint()

docIntelURL = 'https://vat-recogniser.cognitiveservices.azure.com/formrecognizer/documentModels/prebuilt-receipt'
docIntelKey = os.environ["Doc_Intel_Key"]
SQLConnString = os.environ['SQL_Connection_String']
blobConnString = os.environ['blob_Connection_String']


@processDoc.blob_trigger(arg_name="blob", path="receipts", connection=blobConnString) 
def process_doc_blob_trigger(blob: func.InputStream):
    logging.info(f'Python blob trigger function processed a request.\n'
                 f'Name: {blob.name}\n'
                 f'Size: {blob.length} bytes')
    extracted_data = analyse_document(blob)['analyzeResult']
    if (extracted_data == None):
        return func.HttpResponse(
             "Error in extracting document data",
             status_code=400
        )
    
    try:
        parsed_data = parse_data(extracted_data)
    except Exception as e:
        logging.warning(f'Could not parse document contents.{e}')
        return func.HttpResponse(
             "Error in parsing document",
             status_code=400
        )

    try:
        SQL_store_data(parsed_data)
    except Exception as e:
        logging.warning(f'Could not save data to DB.{e}')
        return func.HttpResponse(
                "Error in saving document",
                status_code=400
        )

    return func.HttpResponse(
            f"Your receipt has been uploaded and processed successfully.",
            status_code=200)


def SQL_store_data(parsed_data):
        conn = pyodbc.connect(SQLConnString)
        cursor = conn.cursor()

        # Table should be created ahead of time in production app.
        cursor.execute("""
            INSERT INTO dbo.receipts (transaction_date, amount, VAT)
            VALUES (?, ?, ?)
        """,
        (parsed_data['date'], parsed_data['amount'], parsed_data['vat']))

        conn.commit()
    

def parse_data(receipts):
    output = {'date': '', 'amount': '', 'vat': ''}
    for index, receipt in enumerate(receipts['documents']):
        logging.info("--------Recognizing receipt #{}--------".format(index + 1))
        receipt_type = receipt.get('doc_type')
        if receipt_type:
            logging.info(
                "Receipt Type: {}".format(receipt_type)
            )
        merchant_name = receipt['fields'].get("MerchantName")
        if merchant_name:
            logging.info(
                "Merchant Name: {} has confidence: {}".format(
                    merchant_name.get('content'), merchant_name.get('confidence')
                )
            )
        transaction_date = receipt['fields'].get("TransactionDate")
        if transaction_date:
            output['date'] = transaction_date.get('content')
            logging.info(
                "Transaction Date: {} has confidence: {}".format(
                    transaction_date.get('content'), transaction_date.get('confidence')
                )
            )
        if receipt['fields'].get("Items"):
            logging.info("Receipt items:")
            for index, item in enumerate(receipt['fields'].get("Items").get('valueArray')):
                logging.info("...Item #{}".format(index + 1))
                item_description = item.get('valueObject').get("Description")
                if item_description:
                    logging.info(
                        "......Item Description: {} has confidence: {}".format(
                            item_description.get('content'), item_description.get('confidence')
                        )
                    )
                item_quantity = item.get('valueObject').get("Quantity")
                if item_quantity:
                    logging.info(
                        "......Item Quantity: {} has confidence: {}".format(
                            item_quantity.get('content'), item_quantity.get('confidence')
                        )
                    )
                item_price = item.get('valueObject').get("Price")
                if item_price:
                    logging.info(
                        "......Individual Item Price: {} has confidence: {}".format(
                            item_price.get('content'), item_price.get('confidence')
                        )
                    )
                item_total_price = item.get('valueObject').get("TotalPrice")
                if item_total_price:
                    logging.info(
                        "......Total Item Price: {} has confidence: {}".format(
                            item_total_price.get('content'), item_total_price.get('confidence')
                        )
                    )
        subtotal = receipt['fields'].get("Subtotal")
        if subtotal:
            output['amount'] = subtotal.get('content')
            logging.info(
                "Subtotal: {} has confidence: {}".format(
                    subtotal.get('content'), subtotal.get('confidence')
                )
            )
        tax = receipt['fields'].get("TotalTax")
        if tax:
            output['vat'] = tax.get('content')
            logging.info("Tax: {} has confidence: {}".format(tax.get('content'), tax.get('confidence')))
        tip = receipt['fields'].get("Tip")
        if tip:
            logging.info("Tip: {} has confidence: {}".format(tip.get('content'), tip.get('confidence')))
        total = receipt['fields'].get("Total")
        if total:
            output['amount'] = total.get('content')
            logging.info("Total: {} has confidence: {}".format(total.get('content'), total.get('confidence')))
        logging.info("--------------------------------------")
    return output


def analyse_document(b64_doc):
    analyse_url = docIntelURL + ':analyze?api-version=2023-07-31'
    analyse_res = requests.post(url=analyse_url, json={'base64Source':b64_doc}, headers={'Ocp-Apim-Subscription-Key': docIntelKey})

    if (analyse_res.status_code == 202):
        logging.info('Sent document for analysis.')
        result_id = analyse_res.headers.get('apim-request-id')
        logging.info(result_id)
    else:
        logging.warning(f'Could not analyse document. {analyse_res.text}')
        return
    
    count = 0
    extracted_data = {'status': ''}
    while extracted_data['status'] != 'succeeded' and count < 5:
        if count == 0:
            time.sleep(2)
        else: 
            logging.info('waiting for analysis - sleep until api quota available')
            time.sleep(60)
        result_url = docIntelURL + '/analyzeResults/' + result_id + '?api-version=2023-07-31'
        result_res = requests.get(url=result_url, headers={'Ocp-Apim-Subscription-Key': docIntelKey})

        if (result_res.status_code == 200):
            logging.info('Got response from api.')
            extracted_data = result_res.json()
        else:
            logging.warning(f'Could not get extracted data. {result_res.text}')
            return

    return extracted_data


def encode_file_b64(file):
    try:
        b64_file_bytes = base64.b64encode(file)
        b64_file_string = b64_file_bytes.decode('ascii')
        return b64_file_string
    except:
         return None