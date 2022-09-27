import re

def cleaner(string):  
    #----------Function-Description-----------------
    # INPUT: string - string with strange garbage
    # OUTPUT:  cleaned string
    
    #look for garbage like #{integer}{str} which occurs in the response
    if re.search("\#\d+\w",string):  
        for match in re.finditer("\#\d+\w",string):
            #remove this garbage
            string = string.replace(match.group(), "")
    if re.search("\#\d+",string):  
        for match in re.finditer("\#\d+",string):
            string = string.replace(match.group(), "")
            
    #replace all punctuation with space
    return re.sub(r'[^\w\s]',' ', string)

def list_parser(str_list, keyword):
    #----------Function-Description-----------------
    # INPUT: str_list - string similar to list formatting, keyword - current argument of stage
    # OUTPUT:  final - list of cleaned word
    
    
    #take string of list similar structure -> remove '[' and ']'
    #separate elements of the list by splitting string with ','
    temp = str_list[1:-1].split(",")
    
    #aggregator of processed sentences
    final = []
    
    for i, item in enumerate(temp):
        
        #clean each sentence from grabage and delete unnecessary spaces
        temp[i] = cleaner(item.strip())
        
        #get list of words from the sentence by splitting with space
        #remove empty strings
        list_of_words = list(filter(None, temp[i].split(" ")))
        
        #for Results section take only sentence, ignore individual one word sentences
        if (keyword == "Results" and len(list_of_words)>1) or (keyword != "Results"):
            
            #add cleaned words to aggregator
            final.append(list_of_words)
    return final

def get_parsed_dict(output):
    #----------Function-Description-----------------
    # INPUT: str_list - string similar to list formatting, keyword - current argument of stage
    # OUTPUT: parsed_dict - dict of indicated earlier format
    
    # initialize counter that keep track of current stage in physical plan
    step = 0
    
    # initialize dict
    parsed_dict = dict()
    
    # target arguments + 'Location'
    include = ["Input", "Output", "Results"]
    
    for row in output:
        # only argument rows begin with '('
        if row[0]=="(":
            
            # get current stage number with regular expression
            step = int(re.search("(\d+)", row).group())
            
            #initialize dict for this step
            parsed_dict[step] = dict()
        else:
            # since it is not string with the name of the stage -> look for arguments 
            # identify current argument
            keyword = row.split("[")[0].strip().split(":")[0]
            
            # if this argument is target one, make data cleaner and store 
            if keyword in include:
                
                # "Input", "Output", "Results" have list similar structure => parse as list
                parsed_dict[step][keyword] = list_parser(row.split(":")[1].strip(), keyword)
            elif keyword=="Location":
                
                # get location according to format of the path
                parsed_dict[step][keyword] = row.split("/")[-1].strip()[:-1]             
    return parsed_dict

def get_fields_location(parsed_dict, num_steps):
    #----------Function-Description-----------------
    # INPUT: parsed_dict - our data, num_steps - number of stages of physical plan
    # OUTPUT: list_fields - all column names, locations - sources of these column names
    
    
    # ignore temp columns and system words
    ignore = ['count', 'valueSet', 'sum', 'last', 'first', 'pythonUDF0', 'pythonUDF1', '_groupingexpression']
    
    # agregators for field names and their locations
    list_fields = set() 
    locations = dict()
    
    for i in range(1, num_steps+1):
        # look for names of fields in argument Input
        if "Input" in parsed_dict[i].keys():
            for field in parsed_dict[i]["Input"]:
                if field[0] not in ignore: 
                    list_fields.add(field[0])
        # look for locations of fields in argument Location and set to fields from Output        
        if "Output" and "Location" in parsed_dict[i].keys():
            for f in parsed_dict[i]["Output"]:   
                locations[f[0]] = parsed_dict[i]["Location"]
    
    return list_fields, locations
    
def get_connection_between_fields(parsed_dict, list_fields, num_steps):
    #----------Function-Description-----------------
    # INPUT: parsed_dict - data, list_fields - all fields used while pyspark work, num_steps
    # OUTPUT: edges - connections between columns
    
    
    fields = ["Output", "Results"]
    edges = []
    
    for i in range(1, num_steps+1):
        for f in fields:
            if f in parsed_dict[i].keys():
                if len(parsed_dict[i][f])>1:
                    for item in parsed_dict[i][f]:
                        connection = []
                        for word in item:
                            if word in list_fields:
                                connection.append(word)
                                
                        connection = list(dict.fromkeys(connection))

                        if len(connection):
                            edges.append(connection)
    return edges

def parser(df):
    #----------Function-Description-----------------
    # INPUT: dataframe used by pyspark
    # OUTPUT: graph of dependancies of final columns
    
    
    #--------------Innitial-setup-------------------
    # get pure output of pyspark in 'formatted' mode, so that no information is lost
    # split lines by \n symbol
    # ignore first line, since it is a header
    
    output = df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "formatted").split("\n")[1:]
    
    #remove empty strings
    output = list(filter(None, output))
    
    #get number of steps in physical plan
    num_steps = int(re.search("(\d+)", output[0]).group())
    
    #start analysing output without physical plan graphical representation
    output = output[num_steps:-1]
    
    #--------------Begin-of-parsing-----------------
    
    # Format of parsed_dict
     
    # dict(
    #    'i':{
    #        'Argument_1' : [item1,item2,item3,item4...],
    #        'Argument_1' : [item1,item2,item3,item4...],
    #        'Argument_1' : [item1,item2,item3,item4...]
    #        }
    # Where i is an order of step in physical plan
    # Argument_j is any from ["Input", "Output", "Results", "Location"]
    # item_k is one sample of extracted data per each argument
    
    parsed_dict = get_parsed_dict(output)
    
    
    # list_fields store all unique columns appearing while working pyspark exluding temporary
    # locations stores source of each field in list_fields
    list_fields, locations = get_fields_location(parsed_dict, num_steps)
    
    #--------------Building-of-Graph-----------------
    
    # one column is said to be dependent of another if a different column appears earlier in some query
    # in results or output section
    edges = get_connection_between_fields(parsed_dict, list_fields, num_steps)
                            
    # get list of columns in final table (we just need to get output from the last row)                   
    final_columns = [col[0] for col in parsed_dict[num_steps]["Output"]]
    
    # Format of G is it was proposed in the problem task
     
    # G =
    #    {col1: {data_sources: ["data_source1.csv", "data_source2.csv", ...],
    #            cols_dependencies: [data_source1.col5, data_source2.col4, ...]}
    #     col2: {data_sources: ["data_source4.csv", "data_source2.csv", ...],
    #            col_dependencies: [data_source4.col5, data_source2.col1]},
    #        ...
    #     }
    
    G = dict()
    
    # when we scan .csv or any other file, it appears with the list of columns
    # in Location argument. So, this doesn't depend on any other column
    
    # if there is no source, then the column appeared after some actions, thus depends on
    # other columns 
    
    for key in final_columns:
        G[key] =  {"data_sources": [], "cols_dependencies": []}
        
        if key in locations.keys():
            G[key]["data_sources"].append(locations[key])
        else:
            G[key]["data_sources"] = None
    
    # if there exists an edge between two columns of the table, then one is dependent of the other
    for e in edges:
        for i in e[:-1]:
            
            if i in locations.keys():
                i = locations[i].split(".")[0]+"."+i
            if e[-1] in final_columns:
                G[e[-1]]["cols_dependencies"].append(i)
                    
    return G

