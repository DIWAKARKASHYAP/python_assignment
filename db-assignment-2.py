import psycopg2
import json
from datetime import date

def store_data():
    data = []
    try:
        with open('data_set_python_training.json', 'r') as file:
            data = json.load(file)
    except Exception as e:
        print("Error on line", e.__traceback__.tb_lineno)
        print("Error message:", e)
        return

    try:
        connection = psycopg2.connect(database="python_training", user="postgres", password="1234", host="localhost", port=5432 )
        cursor = connection.cursor()
    except Exception as e:
        print("Failed to connect to the database")
        print("Error message:", e)
        return

    try:
        create_enum_query = "DO $$ BEGIN CREATE TYPE source_enum AS ENUM ('MH', 'WR'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;"
        cursor.execute(create_enum_query)
    except Exception as e:
        print("Error creating ENUM type:", e)
        return


    create_revision_table_query = '''
    CREATE TABLE IF NOT EXISTS schedule_revision_details (
        id BIGSERIAL PRIMARY KEY,
        source_name source_enum NOT NULL,
        data_date DATE NOT NULL,
        revision_no INT NOT NULL,
        created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    '''


    create_transaction_table_query = '''
    CREATE TABLE IF NOT EXISTS schedule_transaction_details (
        id BIGSERIAL PRIMARY KEY,
        sch_rev_details_id INT NOT NULL,
        data_category VARCHAR,
        buyer_name VARCHAR,
        seller_name VARCHAR,
        data_sub_category VARCHAR,
        revision_no INT NOT NULL,
        created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (sch_rev_details_id) REFERENCES schedule_revision_details(id)
    );
    '''

    try:
        cursor.execute(create_revision_table_query)
        cursor.execute(create_transaction_table_query)
        print("Tables created or already exist")
    except Exception as e:
        print("Error creating tables:", e)
        connection.rollback()
        return

    today = date.today()
    print("Today's date is:", today)

    def save_in_rev_table(source_name, revision_no):
        try:
            insert_query = """
            INSERT INTO schedule_revision_details (source_name, data_date, revision_no)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING;  -- Modify this if specific constraints need to be handled
            """
            cursor.execute(insert_query, (source_name, today, revision_no))

            cursor.execute(''' SELECT ID FROM schedule_revision_details WHERE source_name = 'WR' ORDER BY ID DESC''')

            # print(cursor.fetchone())

            return cursor.fetchone()

        except Exception as e:
            print("Error inserting into revision table:", e)
    
    def save_in_trans_table(sch_rev_details_id, data_category, buyer_name,seller_name, data_sub_category, revision_no):
            insert_query = """
            INSERT INTO schedule_transaction_details (sch_rev_details_id, data_category, buyer_name,seller_name, data_sub_category, revision_no)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;  -- Modify this if specific constraints need to be handled
            """
            cursor.execute(insert_query, (sch_rev_details_id, data_category, buyer_name,seller_name, data_sub_category, revision_no))

            # cursor.execute( ''' SELECT ID FROM schedule_revision_details WHERE source_name = 'WR' ORDER BY ID DESC''')

            # print(cursor.fetchone())

    # save_in_trans_table(665, "FULL_SCH", "MSEB_Beneficiary", "GMR_WARORA", "GNA",  1)

    temp_num = 0
    for single_data in data:
        if single_data.get("source_name") == "WR" and single_data.get("sch_data_category") == "EN":

            id_of_rev = save_in_rev_table(single_data["source_name"], temp_num)
            print(id_of_rev)

            save_in_trans_table(id_of_rev , single_data["sch_data_category"],single_data["sch_buyer_name"], single_data["sch_seller_name"],single_data["sch_sub_data_category"], temp_num)

            temp_num = temp_num +1
            # save_in_trans_table()
        # SELECT ID FROM schedule_revision_details WHERE source_name = 'WR' ORDER BY ID DESC


    try:
        connection.commit()
        print("Transaction committed successfully")
    except Exception as e:
        print("Error during commit:", e)
        connection.rollback()

    try:
        cursor.close()
        connection.close()
        print("Database connection closed")
    except Exception as e:
        print("Error closing the connection:", e)


store_data()
