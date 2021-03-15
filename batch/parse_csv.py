import csv
import sys
from datetime import datetime

prefix_dir = "/home/tgkang/data/raw-data/rsn/corona"
filename = prefix_dir + "/KBSTAR_0616_0630_"

outfiles = [[None for i in range(24)] for i in range(31)]

# avoiding error : field larger than field limit (131072)
csv.field_size_limit(sys.maxsize)

for idx in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"]:
    i = 0
    infilename = filename + idx + ".csv"
    print("Open File : ", infilename)
    with open(infilename, 'r') as reader:
        for line in reader:
            i += 1

            try:
                row = csv.reader([line])
                listed_row = list(row)
            # avoiding _csv.Error: line contains NULL byte
            except csv.Error as e: 
                print(i, "'st : Null Data ")
                line = line.replace('\x00', '')
                row = csv.reader([line])
                listed_row = list(row)

            #print(listed_row[0][6])
            try:
                ts = int(listed_row[0][6])
            except:
                print('Unknown : ', line)
                continue
            
            #print(datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))
            dt = datetime.fromtimestamp(ts)

            #print(dt.month, dt.day)
            if outfiles[dt.day][dt.hour] is None:
                outfilename = prefix_dir + "/6/" + str(dt.day) + "/" + str(dt.hour) + ".csv"
                outfiles[dt.day][dt.hour] = open(outfilename, "w")

            outfiles[dt.day][dt.hour].write(line)

            #if i > 10000:
            #    break
    
    print('Total : ', i)

