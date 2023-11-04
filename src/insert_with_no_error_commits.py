import pymysql
import config

# Establish a database connection
connection = pymysql.connect(
    host=config.DB_HOST, user=config.DB_UESR, password=config.DB_PASSWORD,
    db=config.DB_SCHEMA, port=config.DB_PORT
)

cursor = connection.cursor()
batch_size = 100000
offset = 0

try:
    # get total count of origin table
    cursor.execute("""select count(*) from scripts_gt 
            left join files f 
            on scripts_gt.file_id = f.file_id 
            where f.commit_id not in (select start_id from errors_2) 
            """)
    total_count = cursor.fetchone()[0]

    print(f'Total number of data is : {total_count}')

    while offset < total_count:
        cursor.execute("""
        insert into scripts_gt_with_ignore_errors_2 (file_id, change_type, node_type, node_pos, node_length, loc_type, loc_pos, loc_length, change_pos, script)
        select g.file_id, g.change_type, g.node_type, g.node_pos, g.node_length, g.loc_type, g.loc_pos, g.loc_length, g.change_pos, g.script
        from scripts_gt g
        left join files f
        on g.file_id = f.file_id
        where f.commit_id not in (select start_id from errors_2) LIMIT %s OFFSET %s
        """, (batch_size, offset))

        connection.commit()
        print(f"Transferred {offset + batch_size if offset + batch_size < total_count else total_count} of {total_count} records")

        # Update offset
        offset += batch_size

except pymysql.MySQLError as e:
    print("MySQL error:", e)
except Exception as e:
    print("Exception in _query:", e)
finally:
    cursor.close()
    connection.close()
