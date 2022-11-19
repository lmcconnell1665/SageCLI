import logging

logger = logging.getLogger('Sage Logger')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('sage.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
# add the handlers to logger
logger.addHandler(ch)
logger.addHandler(fh)

from xml.dom.minidom import Document, parseString
from dataclasses import dataclass
from datetime import datetime as dt
from datetime import timezone as tz
import time
import os
import requests
import xml.etree.ElementTree as ET
from azure.storage.filedatalake import DataLakeServiceClient

@dataclass
class SageIntacct:
    '''Class representing a Sage Intacct session connection variables'''
    control_id: str = str(time.time()).replace('.', '')
    company_id: str = os.getenv("SAGE_COMPANY_ID")
    user_id: str = os.getenv("SAGE_USER_ID")
    user_password: str = os.getenv("SAGE_USER_PASSWORD")
    sender_id: str = os.getenv("SAGE_SENDER_ID")
    sender_password: str = os.getenv("SAGE_SENDER_PASSWORD")
    url: str = "https://api.intacct.com/ia/xml/xmlgw.phtml"

@dataclass
class AzureDataLake:
    '''Class representing a filesystem client connection credentials to ADLS Gen2 stored in environment variables'''
    storage_account_name: str = os.getenv('AZURE_STORAGE_ACCT_NAME')
    storage_account_key: str = os.getenv('AZURE_STORAGE_ACCT_KEY')
    filesystem: str = os.getenv('AZURE_STORAGE_FILESYSTEM')


@dataclass
class SageResult:
    '''Class holding response data needed to iterate through scans'''
    entity: str
    result_id: str
    this_result_count: int
    number_remaining: int
    total_count: int


def generate_xml_doc(SageSesh, doc_type: str, session_id: str = None, entity: str = None, columns: str = None, where_query: str = None, rows_per_page: str = None, result_id: str = None):
    '''Generates xml for the request object'''

    try:
        new_xml_doc = Document()

        request = new_xml_doc.createElement("request")
        new_xml_doc.appendChild(request)

        # request.control
        control = new_xml_doc.createElement("control")
        request.appendChild(control)

        senderid = new_xml_doc.createElement('senderid')
        control.appendChild(senderid).appendChild(new_xml_doc.createTextNode(SageSesh.sender_id))

        senderpassword = new_xml_doc.createElement('password')
        control.appendChild(senderpassword).appendChild(new_xml_doc.createTextNode(SageSesh.sender_password))

        controlid = new_xml_doc.createElement('controlid')
        control.appendChild(controlid).appendChild(new_xml_doc.createTextNode(SageSesh.control_id))

        uniqueid = new_xml_doc.createElement('uniqueid')
        control.appendChild(uniqueid).appendChild(new_xml_doc.createTextNode("false"))

        dtdversion = new_xml_doc.createElement('dtdversion')
        control.appendChild(dtdversion).appendChild(new_xml_doc.createTextNode("3.0"))

        includewhitespace = new_xml_doc.createElement('includewhitespace')
        control.appendChild(includewhitespace).appendChild(new_xml_doc.createTextNode("false"))

        # request.operation
        operation = new_xml_doc.createElement('operation')
        request.appendChild(operation)

        # request.operation.authentication
        authentication = new_xml_doc.createElement('authentication')
        operation.appendChild(authentication)

        if doc_type == 'Auth':
            login = new_xml_doc.createElement('login')
            authentication.appendChild(login)

            userid = new_xml_doc.createElement('userid')
            login.appendChild(userid).appendChild(new_xml_doc.createTextNode(SageSesh.user_id))

            companyid = new_xml_doc.createElement('companyid')
            login.appendChild(companyid).appendChild(new_xml_doc.createTextNode(SageSesh.company_id))

            password = new_xml_doc.createElement('password')
            login.appendChild(password).appendChild(new_xml_doc.createTextNode(SageSesh.user_password))
        elif doc_type == 'Entity' or doc_type == 'NextPage':
            sessionid = new_xml_doc.createElement('sessionid')
            authentication.appendChild(sessionid).appendChild(new_xml_doc.createTextNode(session_id))

        # request.operation.content
        content = new_xml_doc.createElement('content')
        operation.appendChild(content)

        function = new_xml_doc.createElement('function')
        content.appendChild(function).setAttributeNode(new_xml_doc.createAttribute('controlid'))
        function.attributes["controlid"].value = SageSesh.control_id

        if doc_type == 'Auth':
            getsessionid = new_xml_doc.createElement('getAPISession')
            function.appendChild(getsessionid)
        elif doc_type == 'Entity':
            readbyquery = new_xml_doc.createElement('readByQuery')
            function.appendChild(readbyquery)

            obj = new_xml_doc.createElement('object')
            readbyquery.appendChild(obj).appendChild(new_xml_doc.createTextNode("" + entity + ""))

            fields = new_xml_doc.createElement('fields')
            readbyquery.appendChild(fields).appendChild(new_xml_doc.createTextNode("" + columns + ""))

            query = new_xml_doc.createElement('query')
            readbyquery.appendChild(query).appendChild(new_xml_doc.createTextNode("" + where_query + ""))

            pagesize = new_xml_doc.createElement('pagesize')
            readbyquery.appendChild(pagesize).appendChild(new_xml_doc.createTextNode("" + rows_per_page + ""))
        elif doc_type == 'NextPage':
            readmore = new_xml_doc.createElement('readMore')
            function.appendChild(readmore)

            result = new_xml_doc.createElement('resultId')
            readmore.appendChild(result).appendChild(new_xml_doc.createTextNode(result_id))

    except TypeError:
        logger.error(f"There was invalid variables passed when creating the {doc_type} XML document.")

    pretty_request = request.toprettyxml()
    logger.info(f"The request for this is: \n {pretty_request}")

    return pretty_request


def send_request(payload: str, SageSesh):
    '''Sends an xml request to the sage intacct api endpoint'''

    header = {'Content-type': 'application/xml'}
    num_tries = 3
    while True:
        num_tries -= 1
        try:
            response = requests.post(SageSesh.url, data=payload, headers=header,timeout=600)
            break
        except ConnectionError:
            if num_tries == 0:
                logger.warning("The connection broke during the request. Erroring out.")
                return
                # response = requests.post(SageSesh.url, data=payload, headers=header)
    # response_text = response.text
    # parsed_xml = parseString(response_text)
    # xml_pretty = parsed_xml.toprettyxml()

    # # logger.info(f"The response was: \n {xml_pretty}")

    return response


def parse_session_id(response_object):
    '''Parses the sessionid from the xml response'''

    root = ET.fromstring(response_object.content)

    for child in root.iter('sessionid'):
        session = (child.text)

    for child in root.iter('sessiontimeout'):
        timeout = (child.text)


    logger.info(f'The sesion id will be {session} which times out at {timeout}')

    return session, timeout


def get_new_sesison(SageSesh):
    '''Gets a fresh session token for authentication'''

    request_doc_string = generate_xml_doc(SageSesh, 'Auth')
    response = send_request(request_doc_string, SageSesh)
    result = parse_session_id(response)
    session_id = result[0]

    time_as_string = result[1]

    if ":" == time_as_string[-3:-2]:
        time_as_string = time_as_string[:-3]+time_as_string[-2:]
        timeout = dt.strptime(time_as_string, "%Y-%m-%dT%H:%M:%S%z")

    return session_id, timeout


def get_entity(SageSesh, session_id: str, entity: str, query: str):
    '''Gets a single entity for a specified amount of time (typically 1 month)'''

    request_doc = generate_xml_doc(SageSesh
                                   , 'Entity'
                                   , session_id=session_id
                                   , entity=entity
                                   , columns='*'
                                   , where_query=query
                                   , rows_per_page='1000')
    response = send_request(request_doc, SageSesh)

    return response.text


def get_next_page(SageSesh, session_id: str, result_id: str):
    '''Gets the next page using the result_id'''

    request_doc = generate_xml_doc(SageSesh
                                   , 'NextPage'
                                   , session_id=session_id
                                   , result_id=result_id)
    response = send_request(request_doc, SageSesh)

    return response.text


def initialize_datalake_client(storage_account_name, storage_account_key, file_system='landing-zone'):
    """Creates a service client for the Azure data lake"""
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https", storage_account_name), credential=storage_account_key)

    if service_client:
        logger.info(f"Successfully connected to {storage_account_name} Azure Data Lake")

        # create the filesystem
        filesystem_client = service_client.get_file_system_client(file_system=file_system)
        return filesystem_client
    else:
        logger.warning("Could not connect to Azure Data Lake")
        return False


def upload_to_datalake(filesystem_client, file_name, file_content):
    """Uploads a file to data lake in partitions"""

    logger.info("Creating a file named '{}'.".format(file_name))

    file_client = filesystem_client.get_file_client(file_name)
    file_client.create_file()

    # append data to the file
    # the data remain uncommitted until flush is performed
    logger.info("Uploading data to '{}'.".format(file_name))

    file_client.upload_data(data=file_content, overwrite=True)

    # data is only committed when flush is called
    # file_client.flush_data(len(file_content))

    # read the data back
    # [START read_file]
    download = file_client.download_file()
    downloaded_bytes = download.readall()
    # [END read_file]

    # verify the downloaded content
    if len(file_content) == len(downloaded_bytes):
        logger.info(f"{file_name} was saved successfully.")
        return True
    else:
        logger.error(f"Something went wrong saving {file_name}.")


def save_entity(AzureSesh, file_name, content_to_save):
    '''Saves the entity XML response to the Azure Data Lake'''

    parsed_xml = parseString(content_to_save)
    xml_pretty = parsed_xml.toprettyxml()

    datalake_client = initialize_datalake_client(AzureSesh.storage_account_name, AzureSesh.storage_account_key, AzureSesh.filesystem)

    upload = upload_to_datalake(datalake_client, file_name=file_name, file_content=xml_pretty)

    if upload:
        logger.info(f"Successfully saved {file_name}.")


def check_for_next_entity(response_to_check):
    '''Checks the response for next entity metadata'''

    root = ET.fromstring(response_to_check)  
    for a in root.iter('data'): 

        result = SageResult(entity=a.get("listtype"), result_id=a.get("resultId"), this_result_count=int(a.get("count")), number_remaining=int(a.get("numremaining")), total_count=int(a.get("totalcount")))
    
        logger.info(f"There are {result.number_remaining} records remaining for {result.entity}. Continuing...")
        return result


def main(entity_name: str, query: str, file_name_prefix: str = 'adhoc'):

    # Generate the session dataclass from environment variables
    SageSesh = SageIntacct()
    AzureSesh = AzureDataLake()

    logger.info(f"The {SageSesh.sender_id} sender_id is being used to save to the {AzureSesh.storage_account_name} storage account.")

    if SageSesh.sender_id is None:
        logger.warning("Sage session is missing environmental variables.")

    long_run_start_time = dt.now()

    logger.info(f'Starting function at {long_run_start_time} for {entity_name}.')

    sesh = get_new_sesison(SageSesh)
    session_id = sesh[0]
    timeout = sesh[1]

    entity = get_entity(SageSesh, session_id, entity_name, query)
    save_entity(AzureSesh, f'Sage_Intacct/data_download/{entity_name}/{file_name_prefix}_{entity_name}_0.xml', entity)
    next_entity = check_for_next_entity(entity)

    counter = 1
    while next_entity.number_remaining != 0:

        current_time = dt.now().replace(tzinfo=tz.utc)
        if timeout <= current_time:
            logging.info('Previous token expiring. Getting a fresh session token.')
            sesh = get_new_sesison(SageSesh)
            session_id = sesh[0]
            timeout = sesh[1]

        next_entity_page = get_next_page(SageSesh, session_id, next_entity.result_id)
        save_entity(AzureSesh, f'Sage_Intacct/data_download/{entity_name}/{file_name_prefix}_{entity_name}_{counter}.xml', next_entity_page)

        next_entity = check_for_next_entity(next_entity_page)
        counter += 1
        logger.info(f"Just finished page {counter-1} for {entity_name}.")
        logger.info(f"There are {next_entity.number_remaining} {entity_name} records remaining out of {next_entity.total_count}.")

    long_run_end_time = dt.now() - long_run_start_time
    logger.info(f'Finished processing {entity_name}. It took {counter-1} files and it took {long_run_end_time}')

    return next_entity.total_count, next_entity.number_remaining, counter

if __name__ == '__main__':
    # main()
    main('CUSTOMER', 'WHENMODIFIED >= 06/01/2022 AND WHENMODIFIED <= 07/10/2022')
