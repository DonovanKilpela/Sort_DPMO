# ATLAS 2.0 Pull
# Written: Stephen Slaughter
# Mega help and Midway function from: Alexander Zeledon

import json
import requests
import os
import sys
from openpyxl import load_workbook
import pandas as pd

import calendar
from datetime import datetime, timedelta, timezone
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
from urllib3 import disable_warnings
disable_warnings()

fc = 'DSM5'
process = 'Sort'
subProcess = 'AFE Rebin'

defects = {'Stow': ['Sips Over And Short', 'Multiple Events'],
           'Pick': ['Short', 'Error Indicator'],
           'Sort': ['Opportunities']}


def mw_cookie():
    # Setting defaults for midway cookie location
    path = os.path.join(os.path.expanduser('~'), '.midway')
    cookie = os.path.join(path, 'cookie')

    if not os.path.exists(
            cookie):  # If the cookie doesn't exist, calling mwinit -o --aea (need --aea for ATLAS 2.0)
        os.system('mwinit -o --aea')

    with open(cookie, 'rt') as c:
        cookie_file = c.readlines()  # Reading the cookie file

    cookies = {}
    # Opening the file and looking at timestamp for expired cookie, running
    # mwinit -o --aea again or getting the cookie

    now = datetime.now(timezone.utc).timestamp()  # Timezone-aware

    for line in range(4, len(cookie_file)):
        if cookie_file[line].split('\t')[5] == 'session':
            if int(cookie_file[line].split('\t')[4]) < now:
                os.system('mwinit -o --aea')
                return mw_cookie()
        cookies[cookie_file[line].split('\t')[5]] = str.replace(
            cookie_file[line].split('\t')[6], '\n', '')
    return cookies

def read_data():
    wb = load_workbook('Weekly DPMO Tracker.xlsm')

    controls = wb['Controls']

    DPMO_Under = controls['C2'].value
    Opps_Under = controls['C3'].value

    DPMO_Top = controls['G2'].value
    Opps_Top = controls['G3'].value

    return DPMO_Under,Opps_Under,DPMO_Top,Opps_Top

def atlas_pull(query):
    req = requests.Session()
    cookie_dict = mw_cookie()
    atlas_login = req.get(
        url="https://atlas.qubit.amazon.dev/sso/login",
        auth=HTTPKerberosAuth(mutual_authentication=OPTIONAL),
        verify=False,
        cookies=cookie_dict
    )

    # Add debugging code here

    token = atlas_login.headers['Set-Cookie'].split(';')[0]
    token_dict = {token.split('=')[0]: token.split('=')[1]}
    cookie_dict.update(token_dict)

    fedVer = req.get(
        url=json.loads(atlas_login.text)['authn_endpoint'],
        auth=HTTPKerberosAuth(mutual_authentication=OPTIONAL),
        allow_redirects=True,
        verify=False,
        cookies=cookie_dict
    )

    

    loginRes = req.post(
        url="https://atlas.qubit.amazon.dev/graphql",
        auth=HTTPKerberosAuth(mutual_authentication=OPTIONAL),
        allow_redirects=True,
        verify=False,
        json=query,
        cookies=cookie_dict
    )
    return loginRes


def atlas_update():
    global atlas_df, _ap, postRes, _dpmo_under, _opps_under, _dpmo_top, _opps_top
    atlas_df = pd.DataFrame()
    _dpmo_under, _opps_under, _dpmo_top, _opps_top = read_data()
    # Getting previous hour timestamps
    now = datetime.now()
    
    # Calculate the most recent Saturday
    previous_saturday = now - timedelta(days=(now.weekday() + 2) % 7)
    previous_saturday = previous_saturday.replace(hour=0, minute=0, second=0, microsecond=0)

    # Calcuate the previous Sunday (start of the week)
    previous_sunday = previous_saturday - timedelta(days = 6)

    print("Start hour: ", previous_sunday)
    print("Last hour: ", previous_saturday)
    start_hour = datetime.timestamp(previous_sunday)
    end_hour = datetime.timestamp(previous_saturday)

    to_post = {
        "variables": {
            "warehouseId": fc,
            "department": process,
            "timeRanges": [
                {
                    "startTime": start_hour,
                    "endTime": end_hour}]},
        "query": "fragment ReportParts on Report {  totalsReports {    defectType    processPath    defectCount    opportunities    metricValue    threshold    metricType    __typename  }  managerLevelReports {    managerId    processPath    defectCount    opportunities    metricValue    metricType    __typename  }  rawReports {    processPath    processLevelUniqueMetrics {      displayName      displayNameAlt      __typename    }    processLevelReport {      aggregationField      subProcess      defectMap {        k        v        __typename      }      totalDefects      metricValue      __typename    }    __typename  }  __typename}query ($warehouseId: String!, $department: String!, $subprocess: String, $timeRanges: [TimeRange!]!) {  getReportingByWarehouseId(    warehouseId: $warehouseId    department: $department    subprocess: $subprocess    timeRanges: $timeRanges  ) {    ...ReportParts    __typename  }}"}
    _ap = atlas_pull(to_post)
    postRes = json.loads(_ap.text)

    atlas_df = pd.concat([atlas_df, pd.DataFrame.from_dict(
        postRes['data']['getReportingByWarehouseId']['rawReports'][0]['processLevelReport'])])
    #print(atlas_df['metricValue'])
    main_dict = []

    if process == 'Sort':
        atlas_df = atlas_df.query(
            f'`subProcess` == "{subProcess}"').reset_index(drop=True)

    for i in range(0, len(atlas_df['aggregationField'])):
        aaDefects = pd.DataFrame.from_dict(atlas_df['defectMap'][i])
        
        temp_dict = {'aaLogin': atlas_df['aggregationField'][i].split('-')[1],
                     'DPMO': atlas_df['metricValue'][i]}
        for defect in defects[process]:
            temp_dict.update({defect: aaDefects.query(
                f'`k` == "{[d for d in aaDefects.k if defect in d][0]}"')['v'].values[0]})

        main_dict.append(temp_dict)
    atlas_df = pd.DataFrame.from_dict(main_dict)
    
    
    return filter_underperforming_sort(atlas_df), filter_top_performing_pack(atlas_df)

#_dpmo_under, _opps_under, _dpmo_top, _opps_top

def filter_underperforming_sort(atlas_df):
    print("\nUnderperforming Sorters")
    print("DPMO >", _dpmo_under, "&& Opportunities >", _opps_under)
    sorted_df = atlas_df[(atlas_df['DPMO'] >= _dpmo_under) & (atlas_df['Opportunities'] > _opps_under)].sort_values(by='Opportunities', ascending=False)
    print(sorted_df)

    return sorted_df

def filter_top_performing_pack(atlas_df):
    print("\nTop Performing Sorters")
    print("DPMO < 2,500 && Opportunities > 1,000")
    print("Opportunities Top Variable: ",_opps_top)
    sorted_df = atlas_df[(atlas_df['DPMO'] <= _dpmo_top) & (atlas_df['Opportunities'] > _opps_top)].sort_values(by='Opportunities', ascending=False)
    print(sorted_df)
    return sorted_df

def main():
    atlas_update()

if __name__ == "__main__":
    main()
    sys.exit(0)
