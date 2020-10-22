import requests, csv, json, os, sys
from pathlib import Path
import collections

file = sys.argv[1]

Count=0
my_file = Path(file)
if my_file.is_file():
    API_ENDPOINT = "http://yy.yyy.yy.yyy:8080/xxxxxxx"
    csvfilename = file

    #Read CSV and and add data to a dictionary
    data = {}
    data['bqTable'] = 'bwMonLog'
    bandwidthLogs = []

    def Data_listToDict(lst):
        op = {lst[i].split('=')[0].lstrip(): lst[i].split('=')[1] for i in range(0, len(lst))}
        return op

    # open csv file
    with open(csvfilename, 'rt') as csvfile:

        reader = csv.reader(csvfile, delimiter=',')
        included_cols = [1, 3, 6, 7, 8, 9, 11, 12]

        for row in reader:
                if  row[11].lstrip().upper().startswith("SITENAME=FIN"):
                    content = list(row[i] for i in included_cols)
                    op = Data_listToDict(content)
                    bandwidthLogs.append(op)
                    Count +=1

    grouped = collections.defaultdict(list)
    for item in bandwidthLogs:
        grouped[item['siteName']].append(item)
    for siteName, group in grouped.items():
        data['bandwidthLogs'] = group
        #print (data)
        try:

            r = requests.post(url=API_ENDPOINT, json=data)
            pastebin_url = r.status_code
        except:
            print("Oops! Unable to connect API URL. Try again later...")
            os.system("python MS_TeamsNotification.py API-Link-is-failing-for-bwMonLog")
            os.remove(file)
            sys.exit()

        # print ('bwmonlog')
        if pastebin_url != 201 :
             print ("Incorrect Format")
             os.system("python MS_TeamsNotification.py 'NAS Fail for bwMonLog'")

    print ("No of records Published for bwMonLog are: %i" % Count)
    os.remove(file)

else:
    exit(0)
