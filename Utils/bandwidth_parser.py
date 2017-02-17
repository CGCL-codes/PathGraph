import sys
#inputFile = open("ukunion_io.info")
#outputFile = open("ukunion_sdc1.info", 'w')
inputFile = open(sys.argv[1])
outputFile = open(sys.argv[2], 'w')
index = (int)(sys.argv[3])
timestamp = 0
while 1:
    lines = inputFile.readlines(100000)
    if not lines:
        break
    for line in lines:
        if line.find("sdc1") == -1:
            continue
        timestamp += 1
        a = line.split()
        print a
        outputFile.write(str(timestamp)+':'+a[index]+'\n')
