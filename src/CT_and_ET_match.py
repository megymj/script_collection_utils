import pymysql
import pymysql.cursors
import config

# Establish a database connection
connection = pymysql.connect(
    host=config.DB_HOST, user=config.DB_UESR, password=config.DB_PASSWORD,
    db=config.DB_SCHEMA, port=config.DB_PORT, cursorclass=pymysql.cursors.DictCursor

)

cursor_MAXIMUM_FILE_ID, cursor_a, cursor_b = None, None, None

# Change here with your table name
TABLE_A = 'scripts_gt_with_ignore_errors_2'
TABLE_B = 'scripts_las_with_ignore_errors_2'
TABLE_C_TARGET = 'gt_las_CT_and_ET_match'

try:
    FILE_ID_START = 0
    FILE_ID_END = 50

    # 마지막 file_id 값을 가져와서 MAXIMUM_FILE_ID 저장한다.
    cursor_MAXIMUM_FILE_ID = connection.cursor()
    sql_GET_MAXIMUM_FILE_ID = '''select file_id from files order by file_id desc limit 1'''
    cursor_MAXIMUM_FILE_ID.execute(sql_GET_MAXIMUM_FILE_ID)
    MAXIMUM_FILE_ID = cursor_MAXIMUM_FILE_ID.fetchone()['file_id']

    # Create connection
    cursor_a = connection.cursor()
    cursor_b = connection.cursor()

    while True:
        if FILE_ID_START > MAXIMUM_FILE_ID:
            print('process is end!')
            print('process is end!')
            break

        SQL_SELECT_table_a = f'''
        SELECT *
        FROM {TABLE_A}
        where file_id between {FILE_ID_START} and {FILE_ID_END}
        ORDER BY file_id ASC
        '''

        SQL_SELECT_table_b = f'''
        SELECT *
        FROM {TABLE_B}
        where file_id between {FILE_ID_START} and {FILE_ID_END}
        ORDER BY file_id ASC
        '''

        # get data from a table
        cursor_a.execute(SQL_SELECT_table_a)
        table_a_results = cursor_a.fetchall()

        cursor_b.execute(SQL_SELECT_table_b)
        table_b_results = cursor_b.fetchall()

        for file_id in range(FILE_ID_START, FILE_ID_END + 1, 1):
            table_a_row_list = []
            table_b_row_list = []
            for a_row in table_a_results:
                if a_row['file_id'] == file_id:
                    table_a_row_list.append([a_row['file_id'], a_row['change_type'], a_row['node_type']])
            else:
                # table_a에서 file_id에 해당하는 행이 없으면 다음 file_id로 넘어감
                if not table_a_row_list:
                    continue

            for b_row in table_b_results:
                if b_row['file_id'] == file_id:
                    table_b_row_list.append([b_row['file_id'], b_row['change_type'], b_row['node_type']])
            else:
                # table_b에서 file_id에 해당하는 행이 없으면 다음 file_id로 넘어감
                if not table_b_row_list:
                    continue

            target_len = len(table_a_row_list)
            if target_len != len(table_b_row_list): # 길이가 다른 경우는, 일치하지 않는 경우이다.
                continue
            else:
                common_elements = [element for element in table_a_row_list if element in table_b_row_list]
                if target_len == len(common_elements):
                    cursor_insert = connection.cursor()
                    sql_insert = f'''
                    INSERT INTO {TABLE_C_TARGET} (file_id) VALUES (%s) 
                    '''
                    cursor_insert.execute(sql_insert, file_id)
                    connection.commit()  # 변경 사항 커밋
                    cursor_insert.close()
                    print('processing file_id=',file_id)

        # Update
        FILE_ID_START, FILE_ID_END = FILE_ID_START + 50, FILE_ID_END + 50

except pymysql.MySQLError as e:
    print("MySQL error:", e)
except Exception as e:
    print("Exception in _query:", e)
finally:
    if cursor_MAXIMUM_FILE_ID is not None:
        cursor_MAXIMUM_FILE_ID.close()
    if cursor_a is not None:
        cursor_a.close()
    if cursor_b is not None:
        cursor_b.close()

    if connection:
        connection.close()
