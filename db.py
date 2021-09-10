import psycopg2
import pandas as pd
from utils import ENVIRONMENTAL_VARIABLES, gen_uuid_list
class DatabaseConector:
    def __init__(self):
        self.db_conn = psycopg2.connect(host=ENVIRONMENTAL_VARIABLES['DB_HOST'],
                                        port=ENVIRONMENTAL_VARIABLES['DB_PORT'],
                                        dbname=ENVIRONMENTAL_VARIABLES['DB_NAME'],
                                        user=ENVIRONMENTAL_VARIABLES['DB_USER'],
                                        password=ENVIRONMENTAL_VARIABLES['DB_PASS'])
        self.db_cursor = self.db_conn.cursor()
    
    def run_query(self, query_str, fields = []):
        '''
        This function returns a pandas dataframe when "fields" is a list of headers
        When "fields" is not supplied, it returns the typical tuple returned by postgres query
        '''
        try:
            self.db_cursor.execute(query_str)
            data = self.db_cursor.fetchall()
            self.db_conn.rollback()

            if fields:
                data = pd.DataFrame(data)
                data = data.rename(columns = {i: field for i, field in enumerate(fields)})
                return data

            return data
        
        except Exception as ex:
            self.db_conn.rollback()
            raise Exception(ex)
    
    def get_fields(self, table_name):
        '''
        This function fetches all the field names of a postgresql table and returns as a list
        '''
        s = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';"
        self.db_cursor.execute(s)
        self.db_conn.rollback()
        field_names = [z[0] for z in self.db_cursor.fetchall()]
        return field_names

    def push_dataframe(self, df, table_name):
        """
        This function pushes a pandas dataframe into a postgresql table. The columns of the
        dataframe must match with the field names of the database table.
        """
        # insert id if not present
        if 'id' not in df.columns:
            df['id'] = gen_uuid_list(len(df))
        #rearrrange df columns according to db fields
        fields = self.get_fields(table_name)
        df = df[fields]

        # convert df into tuple of tuples
        data_tuple = tuple(df.itertuples(index=False, name=None))

        # format field names and %s formatter for SQL Query
        field_names = ', '.join(fields)
        formatter = ', '.join(['%s']*len(fields))

        # Execute query
        s = f"INSERT INTO {table_name} ({field_names}) VALUES ({formatter});"
        try:
            self.db_cursor.executemany(s, data_tuple)
            self.db_conn.commit()
        except Exception as ex:
            self.db_conn.rollback()
            raise Exception(ex)

    
    def update_info(self, table_name, id, data: dict):
        '''
        This function updates certain fields into a db table. The field names and values should
        be supplied as key-value pairs in the input argumant "data" which is a dictionary.

        'id' can be either a single id or a non-empty list of ids.
        '''
        update_script = ", ".join([f"{key} = '{data[key]}'" for key in data.keys()])

        if not id:
            print('Warning: ID cannot be null')
            return None
        
        if isinstance(id, list):
            if len(id) > 1:
                s = f"UPDATE {table_name} SET {update_script} WHERE id IN {tuple(id)};"
            else:
                s = f"UPDATE {table_name} SET {update_script} WHERE id = '{id[0]}';"
        else:
            s = f"UPDATE {table_name} SET {update_script} WHERE id = '{id}';"

        try:
            self.db_cursor.execute(s)
            self.db_conn.commit()
        except Exception as ex:
            self.db_conn.rollback()
            raise Exception(ex)
