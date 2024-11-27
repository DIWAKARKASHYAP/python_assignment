import psycopg2
import json



def store_data():

    data=[]
    try:
        with open('data_set_python_training.json', 'r') as file:
            data = json.load(file)
        
    except Exception as e:
        print("Error Line No" , e.__traceback__.tb_lineno)
        print(e)
        return

    connection = psycopg2.connect(database="postgres", user="postgres", password="1234", host="localhost", port=5432)
    cursor = connection.cursor()

    # try:
    #     create_enum_query = "CREATE TYPE source_enum AS ENUM ('MH', 'WR');"
    #     cursor.execute(create_enum_query)
    # except:
    #     print("datatype enum already created")
    
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS schedule_revision_details (
        id BIGSERIAL PRIMARY KEY,
        source_name source_enum NOT NULL,
        data_date DATE,
        revision_no INT NOT NULL,
        created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    '''


    # output = cursor.execute(create_table_query)

    # print(output)


    def save_in_table(source_name,revision_no):
        insert_query = """
        INSERT INTO schedule_revision_details (source_name, revision_no)
        VALUES (%s, %s)
        ON CONFLICT (id) DO NOTHING;
        """  # Adjust this query if your table uses unique constraints
        output = cursor.execute(insert_query, (source_name, revision_no))
        # output3 =cursor.execute(insert_query, ("MH", "29"))
        # print(output2)
    
    # save_in_table()

    for single_Data in data:
        if(single_Data["source_name"] == "WR" and single_Data["sch_data_category"] == "EN"):
            save_in_table(single_Data["source_name"],1)
            # print(single_Data["sch_id"])


    select_query = "SELECT * FROM schedule_revision_details"
    cursor.execute(select_query)
    records = cursor.fetchall()
    print("Data retrieved from database:",records)

    connection.commit()

    # for row in records:
    #     print(row)
   
    # print(data)




store_data()











# 1 - create Database - python_training

# create table :
# shedule_revision_details:

# id (Primary Key) bigserial
# source_name (ENUM can only have 2 values ('MH' or 'WR'))
# data_date (date)
# revision_no (int)
# created_on (timestamp)
# updated_on (timestamp)

# schedule_transaction_details:

# id (Primary Key) bigserial
# sch_rev_details_id - (Foreign key "id" column of schedule_revision_details)
# data_category (varchar)
# buyer_name (varchar)
# seller_name (varchar)
# data_sub_category (varchar)
# revision_no (int)
# created_on  (timestamp)
# updated_on  (timestamp)

# schedule_blockwise_data

# tran_details_id - (Foreign key "id" column of schedule_transaction_details)
# block_no (int)
# block_value (float)
# created_on  (timestamp)






# - Rajat('MH)
# 3- Write a program to insert the records for 'MH' source in schedule_revision_details as mentioned above in 1st problem





# - Diwakar
# 5- same as 3rd problem, write a program to filter out records from dataset based on (source_name = 'MH' objects and  sch_data_category = 'EN') and insert record as shown in 3rd problem.



    # filter order by group by , join ,