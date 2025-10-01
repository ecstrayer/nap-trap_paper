import os
import sqlite3
import hashlib
import itertools

import pandas as pd
import numpy as np
import logging


class DB_Column:

    '''
    Class to mangage column conditionals when using Select_to_SQL.where() method.
    Each column in the database has a corresponding DB_Column object.

    Example:

    a and b are DB_Column objects

    (a == 1)
    Results in the following sql string: 'a.name = 1'

    (a > 1) & (colB < 2)
    Results in the following sql string: 'a > 1 AND b < 2'
    '''

    def __init__(self, name):
        self.name = name
        self.sql_str = []
        self.all_names = [name]

    def get_sql_str(self):
        sql_str = self.sql_str
        if len(self.sql_str) > 1:
            raise Exception(f'Invalid sql string: {sql_str}')
        elif len(self.sql_str) == 0:
            return self.name
        else:
            self.sql_str = []
            return sql_str[0]

    def type_check(self, other):
        if type(other) not in [int, float, str, DB_Column]:
            raise Exception(f'Invalid type: {type(other)}')
        elif type(other) == str:
            other = f'"{other}"'
        elif type(other) == DB_Column:  
            self.all_names += other.all_names
            other = other.get_sql_str()
        return other

    def return_sql_str(self, other, condition):
        other = self.type_check(other)
        self.sql_str.append(f'{self.name} {condition} {other}')
        return self
    
    def between(self, lower, upper):
        self.sql_str.append(f'{self.name} BETWEEN {lower} AND {upper}')
        return self
     
    def like(self, other, exclude = False):
        other =  self.type_check(other)
        if exclude:
            self.sql_str.append(f"{self.name} NOT LIKE {other}")
        else:
            self.sql_str.append(f"{self.name} LIKE {other}")
        return self
    
    def length(self):
        self.name = f'LENGTH({self.name})'
        return self

    def in_(self, other, exclude = False):
        if not (type(other) == list or type(other) == tuple or type(other) == set):
            raise Exception(f'Invalid type: {type(other)}')
        
        other = ','.join([f'"{o}"' if type(o) == str else str(o) for o in other])

        if not exclude:
            self.sql_str.append(f'{self.name} IN ({other})')
        else:
            self.sql_str.append(f'{self.name} NOT IN ({other})')
   
        return self

    def __and__(self, other):
        if other is not self:
            if 'OR' in self.sql_str[0]:
                self.sql_str = ['(' + self.sql_str[0] + ')']
            if 'OR' in other.sql_str[0]:
                other.sql_str = ['(' + other.sql_str[0] + ')']
            self.sql_str += other.sql_str
            other.sql_str = []
            self.all_names.append(other.name)
        
        if len(self.sql_str) < 2:
            raise Exception('Must use & between two conditionals')
        
        self.sql_str = [' AND '.join(self.sql_str)]
        return self
        
    def __or__(self, other):
        if other is not self:
            self.sql_str += other.sql_str
            other.sql_str = []
            self.all_names.append(other.name)

        if len(self.sql_str) < 2:
            raise Exception('Must use | between two conditionals')
        
        self.sql_str = [' OR '.join(self.sql_str)]
        return self

    def __add__(self, other):
        return self.return_sql_str(other, '+')
    
    def __sub__(self, other):
        return self.return_sql_str(other, '-')
    
    def __mul__(self, other):
        return self.return_sql_str(other, '*')
    
    def __truediv__(self, other):
        return self.return_sql_str(other, '/')
    

    def __eq__(self, other):
        return self.return_sql_str(other, '=')

    def __ne__(self, other):
        return self.return_sql_str(other, '!=')

    def __lt__(self, other):
        return self.return_sql_str(other, '<')
    
    def __gt__(self, other): 
        return self.return_sql_str(other, '>')
    
    def __le__(self, other):
        return self.return_sql_str(other, '<=')
    
    def __ge__(self, other):
        return self.return_sql_str(other, '>=')
    
    def __bool__(self):
        raise Exception('Cannot use boolean operators with DB_Column objects')




class Select_to_SQL:

    '''
    Class to generate SQL SELECT queries. Each columns : iterable (columns in database) has a corresponding DB_Column object.
    Queries require a select statement. All other statements are optional.
    
    Examples:

    s is a Select_to_SQL object with columns a, b, and c

    s = Select_to_SQL(['a','b','c'])
    s[['a','b']].where((s['a'] == 1) & (s['b'] < 2))

    generates: SELECT a,b FROM table_name WHERE a = 1 AND  b < 2
    '''
 
    def __init__(self):
        self.reset_query()


    def reset_query(self):
        self.select_columns = None
        self.select_table = None
        self.joins = []
        self.where_str = None
        self.columns_to_group = None
        self.orderby_str = None
        self.limit_str = None
        self.query_columns = []
        self.tables_added = []
        self.column_concat = None
        self.distinct = False 


    def replace_amibiguous_columns(self, tmp_str, ambiguous_col):
        for c in ambiguous_col:
            tmp_str = tmp_str.replace(c[0],c[1]) 
        return tmp_str


    def get_sql_query(self, ambiguous_col = None, reset_query = True):
        
        if self.select_str is None:
            raise Exception('Must provide select statement')
        else:
            if ambiguous_col is not None:
                sql_query = ' '.join([self.replace_amibiguous_columns(s, ambiguous_col) for s in [self.where_str, self.groupby_str, self.orderby_str] if s is not None])
                select_str = self.replace_amibiguous_columns(self.select_str, ambiguous_col)
                sql_query = [s for s in [select_str, self.join_str,  sql_query, self.limit_str] if s is not None]
            else:
                sql_query = [self.select_str, self.join_str, self.where_str, self.groupby_str, self.orderby_str, self.limit_str] 
                sql_query = [s for s in sql_query if s is not None]

            sql_str = ' '.join(sql_query)
            select_columns = self.select_columns
            if reset_query:
                self.reset_query()
            return sql_str, select_columns
        
    def fix_column_call(self, tmp_str, ambiguous_col):
        for c in ambiguous_col:
            tmp_str = tmp_str.replace(c[0],c[1]) 
        return tmp_str
    
    def where(self, col_obj):
        if self.where_str is None:
            self.query_columns += col_obj.all_names
            self.where_str = 'WHERE ' + col_obj.get_sql_str()
        else:
            raise Exception('Where statement already exists')
        return self
    
    def select(self, columns, table = None):

        columns = columns.copy() if type(columns) == list else columns

        for c in columns:
             if c not in self.columns:
                 raise Exception(f'Column {c} not found in database!')

        if self.select_str is not None:
            self.reset_query()

        if table is None:
            table = 'table_name'

        self.query_columns += columns
        self.select_columns = columns
        self.select_table = table

        return self
    
    def check_substr(self, cord):
        if type(cord) ==  int:
            return cord
        elif type(cord) == DB_Column:
            self.query_columns += cord.all_names
            return cord.get_sql_str()
        else:
            raise Exception(f'Invalid type: {type(cord)}')

    def substr(self, column_obj, start, end):

        start = self.check_substr(start)
        end = self.check_substr(end)
        self.query_columns.append(column_obj.name)
        self.select_columns.append(f'SUBSTR({column_obj.name},{start},{end})')
        return self



    @property
    def select_str(self):
        if self.select_columns is not None:
            if not self.distinct:
                return f'SELECT {",".join(self.select_columns)} FROM {self.select_table}'
            else:
                return f'SELECT DISTINCT {",".join(self.select_columns)} FROM {self.select_table}'
        else:
            return None

    @property
    def join_str(self):
        if len(self.joins) > 0:
            return ' '.join(self.joins)
        else:
            return None


    def add_joins(self, join_list, n = 0):

        joins_added = 0
        joins_to_redo = []

        for (t1, c1), (t2, c2) in join_list:
            if t1 not in self.tables_added and t2 in self.tables_added:
                self.joins.append(f'INNER JOIN {t1} ON {t1}.{c1} = {t2}.{c2}')
                self.tables_added.append(t1)
                joins_added += 1
    
            elif t2 not in self.tables_added and t1 in self.tables_added:
                self.joins.append(f'INNER JOIN {t2} ON {t1}.{c1} = {t2}.{c2}')
                self.tables_added.append(t2)
                joins_added += 1

            else:
                joins_to_redo.append([(t1,c1),(t2,c2)])
            
        if len(joins_to_redo) > 0:
            self.add_joins(joins_to_redo)

    def add_func_to_select(self, aggregate_functions):

        self.select_columns = list(self.select_columns)

        for fn, column_list in aggregate_functions.items():
            if fn == 'group_concat':
                self.column_concat = column_list
                
            for col in column_list:
                self.select_columns.append(f'{fn}({col})')
                if col not in self.query_columns:
                    self.query_columns.append(col)

        return self

    @property
    def groupby_str(self):  
        if self.columns_to_group is not None:
            return f'GROUP BY {",".join(self.columns_to_group)}'
        else:
            return None

    def groupby(self, group_list, aggregate_functions = None):
        
        group_list = [g.name if type(g) == DB_Column else g for g in group_list]
        self.columns_to_group = group_list
        self.query_columns += group_list

        if aggregate_functions is not None:
            self.add_func_to_select(aggregate_functions)

        return self

    def orderby(self, order_list, ascending = True):
        self.query_columns += order_list
        order_str = ' ASC' if ascending else ' DESC'
        self.orderby_str = 'ORDER BY ' + ','.join(order_list) + order_str
        return self
    
    def limit(self, limit):
        self.limit_str = f'LIMIT {limit}'
        return self

    def add_table_to_query(self,table_name):
        if self.select_str is None:
            raise Exception('Must provide select statement before adding a table')
        else:
            self.select_table = self.select_table.replace('table_name', table_name)
  
        self.tables_added.append(table_name)


    def __repr__(self):

        if self.select_str is not None:

            try:
                preview_str = '\t'.join(self.select_columns) + '\n'
                for r in self.fetchmany(10):
                    r = [str(x) for x in r] if len(r) > 1 else [str(r[0])]
                    preview_str += '\t'.join(r) + '\n'
                return preview_str

            except Exception as e:
                raise Exception(f'Exception: {" ".join(e.args)}')

        else:
            return 'Empty query with columns: ' + ', '.join(self.columns)


    def __bool__(self):
        raise Exception('Cannot use boolean operators with DB_Column objects')   
       



class DB_SQLite(Select_to_SQL):

    '''
    DB_SQLite is a class that allows users to interact with a sqlite database using sqlite3.
    The class inherits from Select_to_SQL and allows users to generate SQL SELECT queries using the Select_to_SQL class.
    The class also allows users to create new tables, insert data into tables, and drop tables.
    The class does not require users to supply table names. The class will automatically handle simple joins between tables.

    Example:

    db = DB_SQLite('/path/to/database.db', '/path/to/schema.sql')

    #Create a new table
    db.insert('table_name', ['col1','col2'], [(1,2),(3,4)])

    '''

    dtypes = {
        int: 'INTEGER',
        float: 'REAL',
        str: 'TEXT',
        bytes: 'BLOB'
        }
    
    def __init__(self, db_path, schema_path = None, log_status = True, log_path = None, log_level = logging.DEBUG):

        self.db_path = db_path
        self.db_name = os.path.basename(db_path).split('.')[0]
        self.output_dir = os.path.dirname(db_path)
        self.log_status = log_status
        self.log_level = log_level
        if log_status:
            self.log_path = os.path.join(self.output_dir, self.db_name + '.log') if log_path is None else log_path
            logging.basicConfig(filename = self.log_path, level = log_level, format = '%(asctime)s - %(levelname)s - %(message)s')

        if not os.path.exists(db_path):
            if schema_path is not None:
                logging.info(f'Creating new database: {db_path} with schema: {schema_path}')

                with open(schema_path, 'r') as f:
                    schema = f.read()
                    try: 
                        with sqlite3.connect(db_path) as conn:
                            conn.executescript(schema)
                            conn.execute('PRAGMA foreign_keys = ON')
                    except sqlite3.Error as e:
                        os.remove(db_path)
                        raise Exception(f'Exception: {" ".join(e.args)}')
                    
            else:
                raise Exception(f'Database {db_path} does not exist! Please provide schema_path to create a new database.')
    
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.get_foriegn_keys()
        self.build_column_lookups()
        self.update_table_conn()


        super().__init__()



    def new_table(self, table_name, columns, data_types, primary_key = None, foreign_keys = None, temporary = False):

        sql_columns = []

        for i, d in enumerate(data_types):
            if d not in self.dtypes:
                raise Exception(f'Invalid data type: {d}')
            else:
                sql_columns.append(f'{columns[i]} {self.dtypes[d]}')

        if primary_key:
            sql_columns.append(f'PRIMARY KEY ({primary_key})')
        
        if foreign_keys:
            for f in foreign_keys:
                sql_columns.append(f'FOREIGN KEY ({f[0]}) REFERENCES {f[1]}({f[2]})')

        if temporary:    
            self.cursor.execute(f'CREATE TEMPORARY TABLE IF NOT EXISTS {table_name} ({", ".join(sql_columns)})')
        else:
            self.cursor.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(sql_columns)})')


        self.get_foriegn_keys()
        self.build_column_lookups()


    def check_for_ambiguous_columns(self, columns_to_check, table_list):

        columns_to_check = set(columns_to_check)
        columns_to_replace = []

        for i,column in enumerate(columns_to_check):
            col = column.split('(')[-1].strip(')')
            col_tables = set(self.column_lookup[col])
            col_tables = list(col_tables.intersection(set(table_list)))
            if len(col_tables) > 1:
                columns_to_replace.append([column, column.replace(col, f'{col_tables[0]}.{col}')])

        return columns_to_replace


    def fetchone(self, number_to_fetch = 1):
        return self.execute(number_to_fetch = number_to_fetch)
    
    def fetchmany(self, number_to_fetch):
        return self.execute(number_to_fetch = number_to_fetch)
    
    def fetchall(self, number_to_fetch = 'all'):
        return self.execute(number_to_fetch = number_to_fetch)     

    def execute_selection(self, reset_query = True):

        tables, join_str = self.get_tables(self.query_columns)
        self.add_table_to_query(tables[0])

        if self.columns_to_group is not None or join_str is not None:
            ambiguous_col = self.check_for_ambiguous_columns(self.query_columns, tables)
        else:
            ambiguous_col = None

        #need to replace ambigious columns 
        
        if len(tables) > 1:
            self.add_joins(join_str)   

        
        sql_str, select_columns = self.get_sql_query(ambiguous_col = ambiguous_col, reset_query = reset_query)

        return sql_str, select_columns


    def execute(self, sql_str = None, number_to_fetch = None):

        if sql_str is None:
            sql_str, select_columns = self.execute_selection()

        logging.debug(sql_str)

        if number_to_fetch == 'all':
            return self.cursor.execute(sql_str).fetchall()
        elif type(number_to_fetch) == int:
            if number_to_fetch == 1:
                return self.cursor.execute(sql_str).fetchone()[0]
            else:
                return self.cursor.execute(sql_str).fetchmany(number_to_fetch)

        elif number_to_fetch is None:
            self.cursor.execute(sql_str)
        else:
            error_message = f'Invalid number to fetch: {number_to_fetch}. Must be an integer, None, or all'
            logging.debug(error_message)
            raise Exception(error_message)



    def to_list(self):

        sql_str, select_columns = self.execute_selection()
        q = self.execute(sql_str= sql_str, number_to_fetch = 'all')

        try:
            qlen = len(q[0])
        except Exception as e:
            print(q)
            self.reset_query()
            raise Exception(f'{sql_str} \n Error: {e} :This selector is not valid!')

        if qlen == 1:
            return [s for e in q for s in e]
        else:
            return [tuple(e) for e in q]
            


    def to_numpy(self, column_ids = 'all', row_ids = 'all', column_key = None, row_key = None):

        sql_str, select_columns = self.execute_selection()
        row_identifer = select_columns[0]
        column_identifer = select_columns[1]

        if len(select_columns) != 3:
            raise Exception('Please structure query so as follows: db.select(columns = [row_identifier, column_identifier, value])')
        
        column_num = self.get_max(column_identifer) + 1 if column_ids == 'all' else len(column_ids) 
        row_num = self.get_max(row_identifer) + 1 if row_ids == 'all' else len(row_ids)

        
    
        X = np.zeros([row_num, column_num])

        new_row_num = 0
        row_lookup = {}
        new_col_num = 0
        column_lookup = {}
        column_key_lookup = self[column_identifer,column_key].to_dict() if column_key is not None else None
        row_key_lookup = self[row_identifer,row_key].to_dict() if row_key is not None else None

        for ridx, cidx, value in self.execute(sql_str = sql_str, number_to_fetch = 'all'):
            if row_ids != 'all':
                ridx = ridx if row_key is None else row_key_lookup[ridx]
                if ridx not in row_lookup:
                    row_lookup[ridx] = new_row_num
                    new_row_num += 1
                ridx = row_lookup[ridx]

            elif row_key is not None:
                row_lookup = row_key_lookup
     
            
            if column_ids != 'all':
                cidx = cidx if column_key is None else column_key_lookup[cidx]
                if cidx not in column_lookup:
                    column_lookup[cidx] = new_col_num
                    new_col_num += 1
                cidx = column_lookup[cidx] 

            elif column_key is not None:
                column_lookup = column_key_lookup
            X[ridx,cidx] = value
            
            
        return row_lookup,column_lookup, X
    
    

    
    def to_df(self):
        
        sql_str, select_columns = self.execute_selection()
        df = pd.DataFrame(self.execute(sql_str = sql_str, number_to_fetch='all'), columns = select_columns)
        return df


    def to_dict(self, group_by_key = False, key = None):

        sql_str, select_columns = self.execute_selection()

        if len(select_columns) != 2 and key is None:
            raise Exception('Please structure query so as follows: db.select(columns = [key,value]) or provide key')

        if key is not None:
            key = [key] if type(key) == str else key
            key_idx = sorted([select_columns.index(k) for k in key])
        else:
            key_idx = [0]

        q = self.execute(sql_str = sql_str, number_to_fetch='all')
        output_dic = {}

        for x in q:
            x = list(x)
            k = tuple(x.pop(k-i) for i,k in enumerate(key_idx))
            k = k[0] if len(k) == 1 else k
            v = x
            v = v[0] if len(v) == 1 else v
            if k not in output_dic:
                output_dic[k] = [v] if group_by_key else v
            else:
                if group_by_key:
                    output_dic[k].append(v)
                else:
                    raise Exception('There is duplicate keys in query. Resolve or set group_by_key = True')

        return output_dic
        
    def generate_output(self, output, columns, output_type = 'raw'):

        match output_type:
            case 'df':
                return pd.DataFrame(output, columns = columns, index_column = index_column)
            case 'np':
                pass
            
            case 'raw':
                return output


    def insert(self, table_name, columns, values, single = False, primary_key = None, foreign_keys = None, add_table = False):

        if table_name not in self.tables:
            if add_table:
                self.new_table(table_name, columns, [type(v) for v in values[0]], primary_key, foreign_keys)
            else:
                raise Exception(f'{table_name} not in DB!')

        value_str = ', '.join(['?' for n in range(len(columns))])
        column_str = ', '.join(columns)

        try:
            if single:
                self.cursor.execute(f'INSERT INTO {table_name} ({column_str}) VALUES({value_str})', values)
        
            else:
                self.cursor.execute('BEGIN TRANSACTION;')
                self.cursor.executemany(f'INSERT INTO {table_name} ({column_str}) VALUES({value_str})', values)
                
            self.cursor.execute('COMMIT;')

        except sqlite3.Error as e:
            if not single:
                self.cursor.execute('ROLLBACK;')
                logging.critical(f'Exception: {" ".join(e.args)}')
            raise Exception(f' Exception: {" ".join(e.args)}')


    def drop_table(self, table_name):
        
        self.check_table(table_name)
        self.execute(f'DROP TABLE {table_name}')
        self.get_foriegn_keys()
        self.build_column_lookups()

    def check_table(self, table_name):
        if table_name not in self.tables:
            raise Exception(f'Table {table_name} does not exist')


    def filter_tables(self, column_query, tlist, tnames = None, columns = None):


        if tnames is None:
            tnames = [] 

        if columns is None:
            columns = set()

        column_query = column_query.difference(columns)

        if len(column_query) == 0:
            return tnames

        else:
           
            new_columns = set(tlist[0][1])
            new_table = tlist[0][0]
            columns_to_add = new_columns.intersection(column_query)

            if len(columns_to_add) > 0:
                columns = columns.union(columns_to_add)
                tnames.append(new_table)
            
            tnames = self.filter_tables(column_query, tlist[1:], tnames, columns)
    
        return tnames
    

    def sort_tables(self, tmp_tables):

        sorted_tables = {}
        
        for table_name, columns in tmp_tables.items():
            col_num = len(columns)
            if col_num not in sorted_tables:
                sorted_tables[col_num] = []

            sorted_tables[col_num].append(tuple([table_name,columns]))


        for column_number, tables in sorted_tables.items():
            if len(tables) == 1:
                continue
            else:
                new_tables = []
                for t in tables:
                    if t[0].endswith('_iso'):
                        new_tables.append(t)
                    else:
                        new_tables = [t] + new_tables
                           
            sorted_tables[column_number] = new_tables    


        tables = []

        for s in sorted(list(sorted_tables.items()), key = lambda a:a[0], reverse= True):
            tables += s[-1]

        return tables


            
    def get_tables(self, columns):

        tmp_tables = {}
        col = []

        for c in columns:
            c = c.replace('(',')').split(')')
            c = c[1] if len(c) == 3 else c[0]
            col.append(c)

            try: 
                columns_tables = self.column_lookup[c]

                for t in columns_tables:
                    if t not in tmp_tables:
                        tmp_tables[t] = []
                    tmp_tables[t].append(c)
            except:
                self.reset_query()
                raise Exception(f'Column {c} not found in database!')

        tmp_tables = self.sort_tables(tmp_tables = tmp_tables)
        tables = self.filter_tables(set(col), tmp_tables)
        tables_remaining = len(tables)

        if len(tables) == 1:
            return list(tables), None
            
        join_str = []
        tables_added = [tables[0]]

        for t1,t2 in itertools.combinations(tables, r = 2):
            table_path = self.find_path(t1,t2)
            if table_path is None:
                    table_path = self.find_path(t2,t1)
                    if table_path is None:
                        continue
                    elif 'iso' in t1 or 'iso' in t2:
                        table_path = [tuple([t[1],t[0]]) for t in table_path[::-1]]
                                
            all_tables = set([t for p in [[j[0][0],j[1][0]] for j in table_path] for t in p])
            table_bool = set(tables).intersection(all_tables)

            if table_bool == 0 or table_bool == len(all_tables):
                continue

            for j in table_path:
                t1, c1 = j[0]
                t2, c2 = j[1]

                jstr = tuple([(t1,c1),(t2,c2)])
            
                if not jstr in join_str and not jstr[::-1] in join_str:
                    join_str.append(jstr)
                    if t1 not in tables_added:
                        tables_added.append(t1)
                    if t2 not in tables_added:
                        tables_added.append(t2)

            tables_remaining = len(set(tables).difference(set(tables_added)))
            if tables_remaining == 0:
                break
            
        if tables_remaining != 0:
            print(tables, columns, tables_added)
            self.reset_query()
            raise Exception('Selection is not possible!')
        
        return tables_added, join_str


    def find_path(self, t1,t2, prev_tables = None, rlev = 0):
    
        if prev_tables == None:
            prev_tables = [t1]

        try:
            tmp_paths = self.table_conn[t1]
        except:
            return None

        if t2 in tmp_paths:
            return self.get_join(t1,t2)

        for p in tmp_paths:
            if p in prev_tables:
                continue

            try:
                return self.get_join(t1,p) + self.find_path(p,t2, prev_tables, rlev+1)   
            except:
                continue
        rlev -= 1
        return None           


    def find_paths(self,t1, t2, connected_tables = None):

        count = 0

        while True:

            if connected_tables is None:
                new_table = [[t1]]
            else:
                new_table = []
                tnum = len(connected_tables)
                for i in range(tnum):
                    tpath = connected_tables[i]
                    new_table += [tpath + [c] for c in self.table_conn[connected_tables[i][-1]]]

            for t in new_table:
                if t[-1] == t2:
                    tlen = len(t)
                    return [self.get_join(t1 = t[n], t2 = t[n+1]) for n in range(tlen-1)]

            count += 1
            if count == 5:
                raise Exception('These tables are more than 5 apart. Reconsider query')

            connected_tables = new_table


    def get_join(self, t1, t2):
        table_key = tuple(sorted([t1,t2]))
        return [self.foreign_keys[table_key]]


    def get_foriegn_keys(self):
        '''If the name of table ends with _iso then the connections are only uni-directional'''

        self.table_conn = {}
        self.foreign_keys = {}

        for t1 in self.tables: 
            fkeys = self.execute(f'PRAGMA foreign_key_list({t1})', 'all') 
            for f in fkeys:
                t2, t1_col, t2_col = f[2:5]
                self.add_connection(t1, t1_col, t2, t2_col)


    def add_connection(self, t1, t1_col, t2, t2_col):

        unidirectional = False


        if t1 not in self.table_conn:
            self.table_conn[t1] = []

        if t2 not in self.table_conn and not unidirectional:
            self.table_conn[t2] = []

        self.table_conn[t1].append(t2)

        if not unidirectional:
            self.table_conn[t2].append(t1)
        
        table_key = sorted(tuple([tuple([t1,t1_col]),tuple([t2,t2_col])]), key = lambda a:a[0])
        table_names = tuple([table_key[0][0],table_key[1][0]])
        self.foreign_keys[table_names] = table_key


    def update_table_conn(self):

        for column_name, tables in self.column_lookup.items():
            for t1, t2 in itertools.combinations(tables, r = 2):
                self.add_connection(t1, column_name, t2, column_name)

        '''
        for tname, conn in self.table_conn.items():
            if len(conn) == 1:
                continue
            else:
                for t1,t2 in itertools.combinations(conn, r =2):
                    try:
                        _,conn1 = self.foreign_keys[(tname, t1)]
                    except:
                        _,conn1 = self.foreign_keys[(t1, tname)]
                
                    try:
                        _,conn2 = self.foreign_keys[(tname, t2)]
                    except:
                        _,conn2 = self.foreign_keys[(t2, tname)]

                    print(tname, conn1,conn2)

                    if conn1[-1] == conn2[-1]:
                        if t1 not in self.table_conn and t2 not in self.table_conn:
                            self.table_conn[t1] = [t2]
                            self.foreign_keys[tuple(t1,t2)] = [conn1,conn2]

                        elif t1 in self.table_conn:
                            if t2 in self.table_conn[t1]:
                                continue
                            else:
                                self.table_conn[t1].append(t2)
                                self.foreign_keys[tuple([t1,t2])] = [conn1,conn2]
                                
                        else:
                            if t1 in self.table_conn[t2]:
                                continue
                            else:
                                self.table_conn[t2].append(t1)
                                self.foreign_keys[tuple([t2,t1])] = [conn2,conn1]
            '''

    def build_column_lookups(self):
            
        self.column_lookup = {}
        self.table_columns = {}
        self.column_dtype = {}

        for tname in self.tables:
            self.table_columns[tname] = []
            for s in self.execute(sql_str=f'PRAGMA table_info({tname})', number_to_fetch = 'all'):
                column_name, cdtype = s[1:3]
                if column_name not in self.column_lookup:
                    self.column_lookup[column_name] = []
                    self.column_dtype[column_name] = cdtype
                self.column_lookup[column_name].append(tname)
                self.table_columns[tname].append(column_name)

                if cdtype != self.column_dtype[column_name]:
                    col_tables = self.column_lookup[column_name]
                    raise Exception(f'{column_name} in table {col_tables[-1]} is {cdtype}, whereas in table {tname} {self.column_dtype[column_name]}')

    def get_hash(self, sel_string):
        sel_hash = hashlib.md5(sel_string.encode()).hexdigest()
        sel_hash = 'h' + sel_hash
        return sel_hash

    def close(self):
        self.conn.close()


    def __getitem__(self, key):

        ktype = type(key)

        if ktype == str:
            if key not in self.columns and key not in self.table_columns:
                raise Exception(f'Column or table {key} not found in database!')

            if self.select_str is None:
                key = [key] if not key in self.table_columns else self.table_columns[key]
                return self.select(key)
            else:
                return DB_Column(key)
        
        elif ktype == list or ktype == tuple or ktype == set:
            for k in key:
                if k not in self.columns:
                    raise Exception(f'Column {k} not found in database!')
            return self.select(key)
        
        else:
            raise Exception(f'Invalid key type: {ktype}')
        

    @property
    def tables(self):
        table_names = self.execute('SELECT name FROM sqlite_master WHERE type = "table"', 'all')
        return [t[0] for t in table_names]


    @property
    def columns(self):
        return list(self.column_lookup.keys())



    def get_max(self, column_id):

        mval = -1        

        for t in self.column_lookup[column_id]:
            tmval = self.select(columns = [column_id], table = t).add_func_to_select({'MAX':[column_id]}).fetchone()
            mval = mval if tmval is None or mval > tmval else tmval
        return mval




    def get_distinct(self, column_ids):

        distinct_set = set()
    
        for i,c in enumerate(column_ids):
            if i == 0:
                table_names = set(self.column_lookup[c])
            else:
                table_names = unique

        for t in self.column_lookup[column_id]:
            self.distinct = True 
            tdistinct = set([c[0] for c in self.select(columns = [column_id], table = t).fetchall()])
            distinct_set = distinct_set.union(tdistinct)

        return  distinct_set

    @property
    def sql_str(self):
        sql_str, select_columns = self.execute_selection(reset_query = False)
        return sql_str