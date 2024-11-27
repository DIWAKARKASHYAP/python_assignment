import json


def sum_of_all_array(block_data_array):
    output = []
    for index in range(0,96):
        added_Data = 0
        
        for x in block_data_array:
            if(len(x) == 96):
                added_Data = added_Data + x[index]
            else:
                # print(f"blockData have not {len(x) } value ")
                break

        output.append(added_Data) 
    return output


def total_fuel_sum(fuel_type_1 , fuel_type_2):
    data=[]
    try:
        with open('data_set_python_trainig.json', 'r') as file:
            data = json.load(file)
        
    except Exception as e:
        print("Error Line No" , e.__traceback__.tb_lineno)
        print(e)
        return

    all_array_type_1=[]
    all_array_type_2=[]

    for single_data in data:
        if(single_data["cn_fuel_type"] == fuel_type_1 ):
            all_array_type_1.append(single_data["blockData"])
        if(single_data["cn_fuel_type"] == fuel_type_2 ):
            all_array_type_2.append(single_data["blockData"])

    output = sum_of_all_array([sum_of_all_array(all_array_type_1), sum_of_all_array(all_array_type_2)])
        
    return output

   



print(total_fuel_sum("GAS", "COAL"))

