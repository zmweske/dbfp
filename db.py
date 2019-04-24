import operator

MAIN_DATA_TABLE = []
HEADER = ['DocID']
MAX_DOCID = 0
ALL_DOCIDS = []
Q_ERROR = "query semantic error!\n"
D_ERROR = "Duplicate DocID error!\n"
key = []

def parse(line):
#for line in datalines:
    data = [None for x in range(len(HEADER))]
    for datapiece in line.split():
        key = datapiece.split(':')
        if key[0] not in HEADER:
            HEADER.append(key[0])
            # print(HEADER)
        index = HEADER.index(key[0])
        # print(index)
        if len(data) < len(HEADER):
            data.append(int(key[1]))
        else:
            data[index] = int(key[1])
        # if key[0] == 'DocID':
        #     else:
        #         return None
    
    return data

def insert(line):
    data = parse(line)
    global MAX_DOCID

    if data[0] != None:
        if data[0] not in ALL_DOCIDS:
            ALL_DOCIDS.append(data[0])
            MAX_DOCID = max(data[0], MAX_DOCID)
        else:
            return -1
    if data[0] == None:
        MAX_DOCID+=1
        data[0] = MAX_DOCID
        ALL_DOCIDS.append(MAX_DOCID)
    MAIN_DATA_TABLE.append(data)
    return 0

# def printToResults(headerCol, datatable):
#     return

# def throwQueryError():
#     return

# def output(text):
#     print(text)
#     results.

if __name__ == "__main__":
    # import data.txt file into database
    with open('data.txt') as fp:
        contents = fp.read()
        for line in contents.split('\n'):
            if line != "":
                insert(line)



    # read queries.txt
    queries = []
    with open('queries.txt') as fp:
        contents = fp.read()
        for line in contents.split('\n'):
            if line != "":
                queries.append(line)
    
    results = open('results.txt', 'a+')
    
    # process each command
    for command in queries:
        print('>'+command)
        results.write('>' + command + '\n')
        results
        query = command.split('.')
        if query[0] != 'final':
            results.write(Q_ERROR)  # query semantic error
            continue
        del query[0]
        query = query[0].split('(')
        operation = query[0]
        del query[0]

        # query operation
        if operation == 'query':
            if query.count(' ') > 0:
                results.write(Q_ERROR)  # query semantic error
                continue
            query = query[0].split('],[')
            condition = query[0].replace('[','')
            if condition == '':
                condition == False
            else:
                condition = condition.split(',')
            field = query[1].replace(']','').replace(')','')
            if field == '':
                field = []
                for i in range(len(HEADER)-1):
                    field.append(HEADER[i+1])
            else:
                field = field.split(',')
            # print(condition)
            # print(field)
            
            # FIXME process query
            printFields = [0 for x in range(len(HEADER))]
            for column in field:
                if column in HEADER:
                    printFields[HEADER.index(column)] = 1
                # FIXME add else if field is not valid
            # print(printFields)

            #compFields[](doc[],valueFields[])
            valueFields = [None for x in range(len(HEADER))]
            compFields = [None for x in range(len(HEADER))]
            for comparator in condition:
                # pull operator info and column to compare
                for column in HEADER:
                    # swap comparator if in reverse order
                    if comparator.find(column) > 0:
                        if comparator.find('<>') == -1:
                            if comparator.find('<') > -1:
                                comparator = comparator.replace('<','>')
                            elif comparator.find('>') > -1:
                                comparator = comparator.replace('>', '<')
                    if comparator.find(column) != -1:
                        comparator = comparator.replace(column, '')
                        operation = 0
                        if comparator.find('<>') != -1:
                            operation = operator.ne
                        elif comparator.find('>=') != -1:
                            operation = operator.ge
                        elif comparator.find('<=') != -1:
                            operation = operator.le
                        elif comparator.find('>') != -1:
                            operation = operator.gt
                        elif comparator.find('<') != -1:
                            operation = operator.lt
                        elif comparator.find('=') != -1:
                            operation = operator.eq
                        compFields[HEADER.index(column)] = operation
                        comparator = comparator.replace('<','')
                        comparator = comparator.replace('>','')
                        comparator = comparator.replace('=','')
                        valueFields[HEADER.index(column)] = int(comparator)
                        continue
            # print(compFields)
            # print(valueFields)
            for doc in MAIN_DATA_TABLE:
                while len(doc) < len(HEADER):
                    doc.append(None)
                printFlag = True
                for i in range(len(HEADER)):
                    # if compFields[i]:
                    #     docNumStr = doc[i]
                    #     valNumStr = valueFields[i]
                    #     print(str(docNumStr) + ' ' + str(compFields[i]) + ' ' + str(valNumStr))
                    if not (compFields[i] is None):
                        if not (doc[i] is None):
                            if not compFields[i](doc[i], valueFields[i]):
                                printFlag = False
                        else:
                            printFlag = False
                if printFlag:
                    for i in range(len(HEADER)):
                        if printFields[i]:
                            numStr = doc[i]
                            if numStr == None:
                                numStr = 'NA'
                            if not condition and numStr == 'NA':
                                continue
                            results.write(HEADER[i]+':'+str(numStr)+' ')
                    results.write('\n')

        

        # insert operation
        elif operation == 'insert':
            # FIXME should be != count(':')-1, but there is an appending space on each line
            if query.count(' ') != query.count(':'):
                results.write(Q_ERROR)  # query semantic error
                continue
            line = query[0].replace(')', '')
            if insert(line) == -1:
                results.write(D_ERROR)  # duplicate docID error
            else:
                if line.find('DocID') == -1:
                    tempMax = MAX_DOCID
                    results.write('DocID:'+str(tempMax)+' ')
                results.write(line + '\n')


        # count operation
        elif operation == 'count':
            if query.count(' ') > 0:
                results.write(Q_ERROR)  # query semantic error
                continue
            query = query[0].split('],[')
            field = query[0].replace('[', '')
            unique = int(query[1].replace(']', '').replace(')', ''))
            # print(field)
            # print(unique)
            index = HEADER.index(field)
            collected_data = []
            for doc in MAIN_DATA_TABLE:
                if index >= len(doc):
                    continue
                elif doc[index] != None:
                    collected_data.append(doc[index])
            count = len(collected_data)
            if unique == 1:
                count = len(set(collected_data))
            results.write(str(count) + '\n')


        # query semantic error/default action
        else:
            results.write(Q_ERROR)
            continue
        
        print()
        results.write('\n')

    results.close()




    # # print database
    # print(HEADER)
    # for data in MAIN_DATA_TABLE:
    #     print(data)

