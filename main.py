from sshtunnel import SSHTunnelForwarder
from paramiko import RSAKey
from redshift_connector import connect


def connect_redshift():
    try:
        ssh_key = RSAKey.from_private_key_file("pem.key")
        rs_host = "redshift-host"
        rs_port = port
        rs_db = "dev"
        rs_user = "user"
        rs_password = "password"
        tunnel_server_host = "ssh host"
        tunnel_server_port = port
        tunnel_server_user = "user"

        # Start the SSH tunnel
        tunnel = SSHTunnelForwarder(
            (tunnel_server_host, tunnel_server_port),
            ssh_username=tunnel_server_user,
            ssh_pkey=ssh_key,
            remote_bind_address=(rs_host, rs_port),
            local_bind_address=('localhost', 5439)  # Bind to an available local port
        )

        tunnel.start()
        print(f"SSH tunnel established! Tunnel local port: {tunnel.local_bind_port}")

        # Establish a Redshift connection via the SSH tunnel
        connection = connect(
            database=rs_db,
            user=rs_user,
            password=rs_password,
            host='localhost',
            port=tunnel.local_bind_port
        )
        print("Connected to Redshift successfully!")

        return connection, tunnel
    except Exception as e:
        print(f"Error connecting to Redshift: {e}")
        return None, None


def insert_query_into_input_table(data, connection):
    """
    Inserts a new record into the input_table1 and retrieves its ID.
    Adjust this method based on the schema of input_table1.
    """
    cursor = connection.cursor()

    insert_query = """
        INSERT INTO ui_table.input_table (batch_id, parent_query_id, condition_column, condition_operator,
                                             condition_value, logical_operator, is_active, is_parent,
                                             order_position, aggregate_function, query_group_id, constant_clause)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    cursor.execute(insert_query, (
        data['batch_id'], data.get('parent_query_id'), data['condition_column'],
        data['condition_operator'], data['condition_value'], data['logical_operator'],
        data['is_active'], data.get('is_parent', False), data['order_position'],
        data.get('aggregate_function'), data.get('query_group_id'), data.get('constant_clause')
    ))
    
    # Now fetch the ID of the last inserted row
    cursor.execute("SELECT MAX(id) FROM ui_table.input_table;")  # Assuming 'id' is an auto-incrementing primary key
    return cursor.fetchone()[0]  # Return the ID of the last inserted row



def handle_user_input(user_input, connection):
    """
    Handles user input, inserting into the input_table1.
    """
    if user_input.get('is_parent', True):
        # If it's a parent query, insert and retrieve its ID
        parent_id = insert_query_into_input_table(user_input, connection)
        print(f"Parent query inserted with ID: {parent_id}")
    else:
        # For subsequent queries, link them to the parent query
        user_input['parent_query_id'] = user_input.get('parent_id')  # Automatically assign
        insert_query_into_input_table(user_input, connection)
        print("Subsequent query inserted.")


def detect_data_type(value):
    if value is None:
        return 'NULL'

    try:
        if value.lower() in ['true', 'false']:
            return 'boolean'
    except AttributeError:
        pass

    try:
        int(value)
        return 'int'
    except (ValueError, TypeError):
        pass

    try:
        float(value)
        return 'float'
    except (ValueError, TypeError):
        pass

    try:
        from datetime import datetime
        datetime.strptime(value, '%Y-%m-%d')
        return 'date'
    except (ValueError, TypeError):
        pass

    return 'string'


def format_value(value, operator, is_column_comparision=False):
    """
    Formats the condition value based on the operator and whether it's a column comparison.
    """
    # Handle IS NULL and IS NOT NULL without appending 'None'
    if operator in ["IS NULL", "IS NOT NULL"]:
        return ""

    # If it's a column comparison, return the value as is (without quotes)
    if is_column_comparision:
        return f"{value}"

    # Handle BETWEEN operator
    if operator == "BETWEEN":
        start, end = value.split(',')
        return f"'{start}' AND '{end}'"

    # Handle IN and NOT IN operators
    if operator in ["IN", "NOT IN"]:
        values = ", ".join([f"'{v.strip()}'" for v in value.split(',')])
        return f"({values})"

    # Return the value wrapped in quotes if it's a regular string value
    return f"'{value}'"






def map_condition_to_sql_operator(condition):
    condition = condition.lower()
    condition_mapping = {
        'equals': '=',
        'greater_than': '>',
        'less_than': '<',
        'between': 'BETWEEN',
        'in': 'IN',
        'not_in': 'NOT IN',
        'like': 'LIKE',
        'is_not_null': 'IS NOT NULL',
        'is_null': 'IS NULL'
    }
    return condition_mapping.get(condition, condition)


def build_sql_query(data):
    query_parts = []
    active_batches = {}

    # Group queries by batch_id
    for query in data:
        batch_id = query['batch_id']
        condition = f"{query['condition_column']} {map_condition_to_sql_operator(query['condition_operator'])} {format_value(query['condition_value'], query['condition_operator'])}"
        
        # Handle column comparisons (if is_column_comparision is set)
        if query.get('is_column_comparision'):
            condition = f"{query['condition_column']} {map_condition_to_sql_operator(query['condition_operator'])} {query['condition_value']}"

        # Add to existing batch group
        if batch_id not in active_batches:
            active_batches[batch_id] = [condition]
        else:
            # Use logical operator if specified, otherwise default to AND
            logical_operator = query.get('logical_operator', 'AND')
            if logical_operator:
                active_batches[batch_id].append(f"{logical_operator} {condition}")

    # Build SQL query per batch
    for batch_id, conditions in active_batches.items():
        # Join conditions with spaces and ensure proper placement of logical operators
        query_string = f"SELECT * FROM shipments WHERE {' '.join(conditions)}"
        query_parts.append(query_string)

    # Combine queries with UNION (or another operator) between different batch_ids
    final_query = " UNION ".join(query_parts)
    return final_query





def get_table_data(conn):
    try:
        cursor = conn.cursor()
        query = "SELECT * FROM ui_table.input_table;"
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        # Convert list of tuples into a list of dictionaries
        data_dicts = [dict(zip(columns, row)) for row in data]
        
        return data_dicts, columns  # Return both list of dictionaries and columns
    except Exception as e:
        print("Error fetching data from Redshift:", e)
        return None, None  # Ensure consistent return types




def main():
    conn, tunnel = connect_redshift()
    if conn is None:
        return

    # Fetch table data dynamically from Redshift
    data, columns = get_table_data(conn)
    if data is None:
        return

    # Build the SQL query dynamically based on the data from the table
    sql_query = build_sql_query(data)
    print("Generated SQL Query:", sql_query)

    try:
        conn.close()
        print("Connection to Redshift closed.")
        if tunnel and tunnel.is_active:
            tunnel.stop()
            print("SSH tunnel closed.")
    except Exception as e:
        print(f"Error closing the connection or tunnel: {e}")

if __name__ == "__main__":
    main()
