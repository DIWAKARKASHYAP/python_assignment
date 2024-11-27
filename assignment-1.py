import json


def sch_data_category(filterValue):

    if(filterValue == "FULL_SCH" or filterValue == "EN"):
        data = []
        output = []
        with open('data_set_python_training.json', 'r') as file:
            data = json.load(file)
        
        for singleData in data:
            if(singleData["sch_data_category"] == filterValue):
                # print( singleData)
                output.append(singleData)

        print(output)
        

        with open("filtered_data.json", 'w') as json_file:

            print(json_file)
            json.dump(output, json_file, indent=4,  separators=(',',': '))
            
            # print(file)

    else:
        print("please give the right argument")
        return


sch_data_category("EN")