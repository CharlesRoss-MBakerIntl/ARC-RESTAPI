import requests
import json
import pandas as pd
import pytz

import warnings
warnings.simplefilter('ignore')




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
    



############################################## GRAB THE OID FIELD FOR A FEATURE LAYER #######################################################

def oid_field(service_url, layer, token):

    #Set Query URL
    url = f'{service_url}/{str(layer)}/query'

    #Set Query Params
    params = {
        'f': 'json',
        'where':'1=1',           # response format
        'outFields': '*',       # fields to include in the response
        'returnGeometry': 'false',
        'token':token  # don't include geometry
    }

    #Send Response to AGOL
    response = requests.get(url = url, params=params)

    #Pull Table Data
    data = json.loads(response.text)


    #Search for OID Field in Table
    oid_field = None

    for field in data['fields']:
        if field['type'] == 'esriFieldTypeOID':
            oid_field = field['name']
            break

    return oid_field



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

def agol_table_to_pd(service_url, layer, token, convert_dates = "n", drop_objectids = "n"):

    url = f'{service_url}/{str(layer)}/query'

    #Enter Serach Parameters to Pull Data Table
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



    #Drop ObjectID
    if drop_objectids.lower() == "y":
    
        if "ObjectId" in df.columns:
            df = df.drop(columns = "ObjectId")

        elif "objectid" in df.columns:
            df = df.drop(columns = "objectid")

        elif "OBJECTID" in df.columns:
            df = df.drop(columns = "OBJECTID")

        elif "Fid" in df.columns:
            df = df.drop(columns = "Fid")

        elif "fid" in df.columns:
            df = df.drop(columns = "fid")

        elif "FID" in df.columns:
            df = df.drop(columns = "FID")


    #Catch All Date Fields and Convert to Pandas Datetime if Selected
    if convert_dates.lower() == "y":
        agol_date_convert_akt(data, df)
    
    elif convert_dates.lower() == "n":
        pass

    else:
        pass


    #Fill NAs or Nans
    df.fillna("", inplace = True)
    

    return df





###################################################### CONVERT AGOL SERVICE URL TO PANDAS DF WITH GEOMETRY #############################################################

def agol_table_to_pd_with_geometry(service_url, layer, token, uid_field, uid = "*"):

    #Set Query URl
    url = f'{service_url}/{str(layer)}/query'

    if uid != "*" and type(uid) == int:
        
        #Enter Serach Parameters to Pull Data Table
        params = {
        'where': f'{uid_field}={uid}',
        'outFields': '*',
        'returnGeometry': 'true',
        'f': 'geojson',
        'token':token
        }

   
    elif uid == "*":
        #Enter Serach Parameters to Pull Data Table
        params = {
        'where':"1=1",
        'outFields': '*',
        'returnGeometry': 'true',
        'f': 'geojson',
        'token':token
        }


    else:
        raise Exception("UID is not an Integer")
    

    #Send Pull Request
    response = requests.get(url, params)

    # Extract the geometry from the response
    data = response.json()
    geometry = [feature['geometry'] for feature in data['features']]


    return data, geometry








#################################################### ADD, UPDATE, DELTE REST API CONTROL ###########################################################
def add_update_del_agol(mode, url, layer, token,  data):

    #Set applyEdits URL
    service_url = f'{url}/{str(layer)}/applyEdits'

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
                        "updates": f'{data}'.replace("None", 'null')
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
    query_url = f"{service_url}/{str(layer)}/query"

    query_params = {
        'f':'json',
        'token':token,
        'where':f"{uid_field}='{uid}'",
        'outFields' : f"{objectid_field}"
    }

    query_response = requests.get(query_url, query_params).json()
    objectid = query_response['features'][0]['attributes'][objectid_field]

    return objectid





############################################## CATCH SUCCESS OR ERROR MESSAGES FROM ARC REST API RESPONSE #######################################################

def catch_response():
    #Complile List of Responses and Return List
    pass


