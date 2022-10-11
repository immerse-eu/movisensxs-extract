# Required Libraries
# - install packages: pandas, PyYAML, pyncclient, requests
import requests
import json
import os
import pandas as pd
import zipfile
import yaml

# Read configuration file
with open("config.yaml", "r") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

# Initialize dictionary for result status
dictResults = {}

for studyKey in config['studies']:

    print(studyKey, config['studies'][studyKey]['id'], config['studies'][studyKey]['apikey'])

    # Create URL for downloading data of current study
    studyUrl = config['movisensXS']['baseUrl'] + str(config['studies'][studyKey]['id']) + '/results'

    # Download study data in JSON format
    responseJson = requests.get(studyUrl, headers={"Accept": "application/json", "Authorization": 'ApiKey ' + config['studies'][studyKey]['apikey']})

    # Download study data in Excel format
    responseExcel = requests.get(studyUrl, headers={"Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "Authorization": 'ApiKey ' + config['studies'][studyKey]['apikey']})

    # Log Result codes
    dictStudyResult = {
        "status_forms_json": responseJson.status_code,
        "status_forms_excel": responseExcel.status_code
    }
    dictResults[studyKey] = dictStudyResult

    # Print status message for current study
    print(studyKey, config['studies'][studyKey]['id'], config['studies'][studyKey]['apikey'], studyUrl, 'Json: ', responseJson.status_code, "Excel: ", responseExcel.status_code)

    # Check if local directory exists for current study and create it if not
    localStudyPath = config['localPaths']['basePath'] + '/' + str(studyKey)
    if not os.path.isdir(localStudyPath):
        os.mkdir(localStudyPath)

    # Save JSON- & Excel downloads in the study directory
    with open(localStudyPath + '/' + studyKey + '.json', "w") as text_file:
        text_file.write(responseJson.text)
    with open(localStudyPath + '/' + studyKey + '.xlsx', "wb") as text_file:
        text_file.write(responseExcel.content)

    # Generate URL for downloading participants of current study
    studyUrlProbands = config['movisensXS']['baseUrl'] + str(config['studies'][studyKey]['id']) + '/probands'

    # Download participants of current study in JSON format
    responseProbands = requests.get(studyUrlProbands, headers={"Accept": "application/json", "Authorization": 'ApiKey ' + config['studies'][studyKey]['apikey']})
    jsonProbands = json.loads(responseProbands.text)
    dfProbands = pd.json_normalize(jsonProbands)

    # Iterate over the participants of the current study for Unisens data
    dictProbandResult = {}
    for indexProband, rowProband in dfProbands.iterrows():

        # Only query data for participants not in "uncoupled" state
        if rowProband['status'] != 'uncoupled':

            # Generate URL for downloading current participant data
            studyUrlUnisens = config['movisensXS']['baseUrl'] + str(config['studies'][studyKey]['id']) + '/probands/' + str(rowProband['id']) + '/unisens'

            # Download Unisens file for the current participant
            responseUnisens = requests.get(studyUrlUnisens, headers={"Authorization": 'ApiKey ' + config['studies'][studyKey]['apikey']})

            # Log Result codes
            dictProbandResult[rowProband['id']] = responseUnisens.status_code

            # Print status message for the current participant
            print('- ', rowProband['id'], studyUrlUnisens, 'Status: ', responseUnisens.status_code)

            # Save Unisens data on successful download
            if responseUnisens.status_code == 200:

                # Check if a local directory already exists for current study/participant and create it if not
                unisensPath = localStudyPath + '/' + studyKey + '-' + str(rowProband['id'])
                if not os.path.isdir(unisensPath):
                    os.mkdir(unisensPath)

                # Save Unisens data in local directory
                with open(unisensPath + '.zip', "wb") as text_file:
                    text_file.write(responseUnisens.content)

                # Unpack Unisens.ZIP file into the participant directory
                with zipfile.ZipFile(unisensPath + '.zip', 'r') as zip_ref:
                    zip_ref.extractall(unisensPath)

        # Log participant results
        dictResults[studyKey]["unisens_status"] = dictProbandResult

with open(config['localPaths']['basePath'] + "/export_results.yaml", 'w') as outfile:
    yaml.dump(dictResults, outfile, default_flow_style=False)
