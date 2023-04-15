import requests
import json
import pandas as pd
import pytz



############################################################ CREATE LOG IDS IN TABLES ######################################################################

def create_log_ids(df, id_field, date_field):
    
    #Check for existing Log_ID Field
    if "LOG_ID" not in df.columns:

        #Check if Date Field in Columns
        if id_field in df.columns:
            #Check if ID Field in Columns
            if date_field in df.columns: 

                #Iterrate through DF and Create Log ID for Each Column
                for index, row in df.iterrows():
                    
                    log_id = str(df.loc[index, id_field]) + "-" + str(df.loc[index, date_field])
                    df.loc[index, "LOG_ID"] = log_id

                #Return DF with LOG IDS
                return df

            #Date Field Not in int64 Format, Convert before Continuing
            else:
                raise Exception("Date Field not in Dataframe")

        #Date Field not Found
        else:
            raise Exception("ID Field not in Dataframe")
        





############################################################ CONVERT AGOL DATES TOOL ######################################################################

def agol_date_convert_akt(agol_data, agol_df):

    #Set Alaska Timezone
    alaska_tz = pytz.timezone('US/Alaska')
    
    #Pull Fields from AGOL Data Table, BEFORE PD CONVERSION
    if agol_data.get("fields") != None:
        fields = agol_data['fields']

        #Find Field Names and Types
        field_types = pd.DataFrame([[row['name'], row['type']] for row in fields], columns = ['name', 'type'])

        #Iterate Through Data Field, if Field is an ESRIDATETYPE, Check if Field in AGOL DF, If There, Convert to Datetime
        for index,row in field_types.iterrows():
            if row['type'] == 'esriFieldTypeDate':
                date_field = row['name']
                if date_field in agol_df.columns:
                    agol_df[date_field] = pd.to_datetime(agol_df[date_field], unit='ms')
                    agol_df[date_field] =  agol_df[date_field].dt.tz_localize('UTC').dt.tz_convert(alaska_tz)
                    agol_df[date_field] = agol_df[date_field].apply(lambda x: x.strftime('%B %d, %Y   %H:%S'))

        return agol_df

    elif agol_data.get("fields") == None:
        raise Exception("Input Data Table Has No 'Fields' Attribute")
    





######################################################## COMPARE COLUMNS BEFORE SUBMITTING TO AGOL ######################################################################

def columns_compare(org_df, new_df):
    
    #Check if Column Lengths Match
    if len(org_df.columns) != len(new_df.columns):
        raise Exception(f"""
        WARNING!: Dataframe Size Does Not Match
        
        Org DF Column Length: {len(org_df.columns)}   New DF Column Length: {len(new_df.columns)}
        """)

    #Check if Column Names Match
    if org_df.columns.equals(new_df.columns) == False:
        raise Exception(f"""
        WARNING!: Column Names Do Not Match
        
        Org DF Columns: {org_df.columns} 
        New DF Columns: {new_df.columns}
        """)

    #Check if Column Types Match
    for column in org_df.columns:
        org_type = org_df[column].dtype
        new_type = new_df[column].dtype

        if org_type != new_type:
            raise Exception(f"""
            WARNING!: Column Types Do Not Match

            {column}: {org_type}    {column}: {new_type}
            """)






###################################################### CONVERT PANDAS TO ATTRIBUTE LIST FOR AGOL UPLOAD #############################################################

def pd_to_attributes_list(df):
    
    #Create Entry List for Attributes
    data_append = []


    #Check for Dates in Table and Convert to Strings
    if df.select_dtypes(include=['datetime64']).columns != 0:
        dates = df.select_dtypes(include=['datetime64']).columns
        for column in dates:
            df[column] = df[column].astype("str")


    #Iterate Throught the Entire Pandas Data Table
    for row in df.iterrows():
    
        #Grab Each Row
        entry = pd.DataFrame(data = row[1])

        #Convert the Row into a Dictionary
        entry = entry.to_dict()
        
        #Grab Info from Dictionary and Place into Attributes Dictionary
        for key, values in entry.items():
            attributes = {'attributes': values}

        #Add Attributes to List of Items to Append to Hosted Table
        data_append.append(attributes)

    return data_append





###################################################### GENERATE AGOL TOKEN #############################################################

def token_generation(username, password):
    #Rest Api Token URl
    url = 'https://www.arcgis.com/sharing/rest/generateToken'

    #User Data to Generate Token
    data = {
        "username":username,
        'password':password,
        'referer':'https://www.arcgis.com'
    }

    #Additional Parameters
    params = {
        'f':'json'
    }

    #Send Response to Generate Token
    response = requests.post(url, params=params, data=data)

    #Save Token
    token = response.json()["token"]

    return token






###################################################### CONVERT AGOL SERVICE URL TO PANDAS DF #############################################################

def agol_table_to_pd(service_url, layer, token, geometry = "N", convert_dates = "n", drop_objectids = "n"):

    url = f'{service_url}/{str(layer)}/query'

    #Enter Serach Parameters to Pull Data Table
    if geometry.lower() == "y":
        params = {
            'f': 'json',
            'token': token,
            'returnGeometry':True,
            'returnAttachments':True,
            'where': '1=1',  
            'outFields': '*',
        }

    elif geometry.lower() == "n":
        params = {
            'f': 'json',
            'token': token,
            'where': '1=1',  
            'outFields': '*',
        }

    #Send Repsonse to Pull Table
    response = requests.get(url, params=params)

    #If Response Connection Successful, Pull Data and Convert to Pandas Dataframe
    if response.status_code == 200:
        data = response.json()
        table = data.get('features', [])
        df = pd.DataFrame([row['attributes'] for row in table])


        if drop_objectids.lower() == "y":
        #Drop ObjectID
            if "ObjectId" in df.columns:
                df = df.drop(columns = "ObjectId")

            elif "objectid" in df.columns:
                df = df.drop(columns = "objectid")

    #Report Error
    else:
        raise Exception("Failed to Pull Table  -  " + str(response.status_code))


    #Catch All Date Fields and Convert to Pandas Datetime if Selected
    if convert_dates == "Y":
        agol_date_convert_akt(data, df)
    
    elif convert_dates == "N":
        pass

    else:
        pass

    return df






###################################################### ADD NEW LOGS TO AGOL #############################################################

def add_new_logs(new_logs_df, table_service_url, token):

    """
    Add new log entries to the log book hosted in arcgis online. Add the pandas dataframe version of the new log entry, the 
    table service url found on the tables arcgis online page, and a generated token from the ArcGIS REST API.
    """

    #Create Empty Message to be Returned at End of Process
    message = ""
    
    #Convert the New Logs to JSON Attributes
    log_to_attr = pd_to_attributes_list(new_logs_df)

    #Pull Existing Log Ids
    log_ids = agol_table_to_pd(table_service_url, token)['LOG_ID'].to_list()

    #Create an Empty New Log List to Add to the Hosted Table
    new_logs = []

    #Create Empty List to Store Log IDS for Later Check
    new_log_check = []

    #Create a List of the New Log IDS
    for entry in log_to_attr:
        
        for attributes in entry.values():
            
            #Grab LOG ID and Place into List for Later Check
            log_id = attributes['LOG_ID']
            new_log_check.append(log_id)

            #Check if LOG ID Already Exists in Existing Table, If Not, Add to New Logs List
            if log_id not in log_ids:
                new_logs.append(entry)
    

    #Check if New Logs Empty, if Not, Proceed to Upload Data to Hosted Table
    if new_logs != []:

        #Service URL to APPEND DATA
        if "/0/addFeatures" not in table_service_url:
            features_service_url = table_service_url + '/0/addFeatures'
        
        elif "/0/addFeatures" in table_service_url:
            features_service_url = table_service_url

        params = {'f': 'json', 'token': token}

        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        data = {'f': 'json', 'token': token, 'features': json.dumps(new_logs)}
        
        response = requests.post(features_service_url, params=params, headers = headers, data = data)


        #Confirm New Logs have been Added to the Hosted Table
        check_log_ids = agol_table_to_pd(table_service_url, token)['LOG_ID'].to_list()
        
        #Create a Check System to Ensure all Log IDs have been entered
        check = []

        for log_id in new_log_check:
            if log_id in check_log_ids:
                check.append(True)
            elif log_id not in check_log_ids:
                check.append(False)

        if False not in check:
            message = "New Log Entries Successfully Added to Log Book"

        elif False in check:
            raise Exception("New Log IDs Not in Updated Hosted Table, Please Try Again")

        
    #Log ID's Already Located in Log Book
    elif new_logs == []:
        
        message = "Log IDs Already Present in Log Book"


    return message



#################################################### ADD, UPDATE, DELTE REST API CONTROL ###########################################################
def add_update_del_agol(mode, url, layer, token,  data):

    #Set applyEdits URL
    service_url = f'{url}/{layer}/applyEdits'

    #Add Data to Table
    if mode == "add":

        package = pd_to_attributes_list(data)

        add_params = {
        'f':'json',
        "token": token,
        "adds": [package],
        }

        #Send the Request to Add Features
        add_response = requests.post(service_url, params= add_params)

        #Return Results
        return add_response.json()
    
    

    #Update Records in Table
    elif mode == 'update':

        update_params = {
                        'f':'json',
                        'token':token,
                        "updates": f'{data}'
                        }

        update_response = requests.patch(service_url, update_params)

        return update_response.json()




    #Delete Records in Table
    elif mode == "delete":

        del_params = {
            'f':'json',
            'token': token,
            'deletes': data
            }

        del_response = requests.post(service_url, del_params)

        return del_response.json()




################################################### LOCATE OBJECTID BASED ON UID ##########################################################

def locate_objectid(service_url, layer, token, uid_field, uid, objectid_field):
    query_url = f"{service_url}/{layer}/query"

    query_params = {
        'f':'json',
        'token':token,
        'where':f"{uid_field}='{uid}'",
        'outFields' : f"{objectid_field}"
    }

    query_response = requests.get(query_url, query_params).json()
    objectid = query_response['features'][0]['attributes']['ObjectId']

    return objectid





############################################## CATCH SUCCESS OR ERROR MESSAGES FROM ARC REST API RESPONSE #######################################################

def catch_response():
    #Complile List of Responses and Return List
    pass
