r"""


"""

#Imported Modules
import threading
import time 
import sys 
import getopt
import datetime
import MySQLdb
import time
import traceback

__all__ = ["DBOperateAction", "MySQLGlobal",]

#
INCREMENT = 1
FULLDOSE = 2


class DBOperateAction():
    """MYSQL Operation class
	   
	   An instance of this class represents a connection to a MYSQL
    server.  The instance is initially not connected; the connect()
    method must be used to establish a connection.  Alternatively, the
    database host, account, password, database and optional port number 
	can be passed to the constructor, too.
	
	"""
    def __init__(self, dbhost,dbaccount,dbpasswd,dbname, dbport=3306):
	    """Constructor

        When called, create an unconnected instance.The host, account, 
		password and database are must; port number is optional
		
		"""
        self.dbhost=dbhost
        self.dbaccount=dbaccount
        self.dbpasswd=dbpasswd
        self.dbname=dbname
        self.port = dbport
        self.db_conn = None
        self.db_cursor = None

    def __del__(self):
        """Destructor -- close the connection."""
        self.close()		
		
    def close(self):
	    """Close the connection"""
        self.db_conn.close()
        self.db_cursor.close()

    def connect(self):
	    """Connect to the database.

        Don't try to reopen an already connected instance.
		
		"""
        try:
            self.db_conn=MySQLdb.connect(self.dbhost,self.dbaccount,self.dbpasswd,charset="utf8")
            self.db_cursor=self.db_conn.cursor()
            return True
        except MySQLdb.OperationalError:
            return False


    def re_connect(self):
	    """Reconnect to the database if the connection is broken
		"""
        try:
            self.db_conn=MySQLdb.connect(self.dbhost,self.dbaccount,self.dbpasswd,self.dbname,charset="utf8")
            self.db_cursor=self.db_conn.cursor()
            return True
        except MySQLdb.OperationalError:
            return False


    def query(self, sql):
	    """Query a SQL from the Database
		
		The parameter sql should be a valid SQL expression
		
		"""
	
        try:
            self.db_conn.query(sql)
            result = self.db_conn.store_result()
        except MySQLdb.OperationalError:
            traceback.print_exc()
            for i in range(0,3):
                time.sleep(5)
                if self.re_connect():
                    break

        
    def get_all_result(self,sql):
	    """Return all the results queried by the SQL expression
		
		The parameter sql should be a valid SQL expression		
		
		"""
        time.sleep(0.5)
        try:
            self.db_cursor.execute(sql)
            #self.db_conn.commit()
            #print(self.db_conn.store_result())
            #print(self.db_conn.use_result())
            result=self.db_cursor.fetchall()
            self.db_cursor.nextset()
            self.db_conn.commit()

            #print(result)
            return result
        except MySQLdb.OperationalError:
            traceback.print_exc()
            for i in range(0,3):
                time.sleep(5)
                if self.re_connect():
                    self.get_all_result(sql)
                    break
        except MySQLdb.ProgrammingError as e:
            traceback.print_exc()
            print(e.message)
            
    
    def get_one_result(self,sql):
	    """Return the first result queried by the SQL expression
		
		The parameter sql should be a valid SQL expression	
		"""
        try:
           self.db_cursor.execute(sql)
           result=self.db_cursor.fetchone()
           self.db_conn.commit()
           return result
        except MySQLdb.OperationalError:
            traceback.print_exc()
            for i in range(0,3):
                time.sleep(5)
                if self.re_connect():
                    break
        except MySQLdb.ProgrammingError as e:
            traceback.print_exc()


class MySQLGlobal(threading.Thread):
    """MYSQL status analysis class

	   An instance of this class represents a connection to a MYSQL
    server.  The instance is initially not connected; the connect()
    method must be used to establish a connection.  Alternatively, the
    database host, account, password, database and optional port number 
	can be passed to the constructor, too.
	
    """
    def __init__(self, thread_name, host, user, password, port=3306, interval=5, count = 0, filename=None, mode=FULLDOSE):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._db = ''
        self._interval = 5
        self._sleep_time = interval
        self._count = count
        self._file = filename
        self._mode = mode
        threading.Thread.__init__(self, name = thread_name)
        self.mysql_conn = None
        self._pre_status_value = {}

        self._status = {        }



    def run(self):
        _show_status = "SHOW GLOBAL STATUS;"
        _show_variables = "SHOW GLOBAL VARIABLES;"
        _show_processlist = "SHOW FULL PROCESSLIST;"

        self.mysql_conn = DBOperateAction(self._host, self._user, self._password, self._port)
        self.mysql_conn.connect()

        global_variables = self.mysql_conn.get_all_result( _show_variables)
        self.mysql_variables_process( global_variables)

        #for i in xrange(1):
        #self._count is 0 loop
        self._count -= 1
        frp = None
        if self._file:
            frp = open(self._file,"a")

        #Init the first Query
        if self._mode is INCREMENT:
            global_status = self.mysql_conn.get_all_result(_show_status)
            self.mysql_status_process(global_status, mode=INCREMENT)
			
        #Wait 1 second and query another time
        time.sleep(1)

        while True:
            global_status = self.mysql_conn.get_all_result(_show_status)
            #print(global_status)
            self.mysql_status_process( global_status, file_handler= frp, mode=self._mode)
            if self._count is 0:
                break
            self._count -= 1
            time.sleep(self._sleep_time)

        if frp:
            frp.close()

        global_processlist = self.mysql_conn.get_all_result( _show_processlist)
        self.mysql_processlist_process( global_processlist)

        self.mysql_conn.close()



    def mysql_processlist_process(self, processlist):
        #_processlist = dict(processlist)
        #print(processlist)
        pass

    def mysql_variables_process(self, variables):
        self._global_variables = dict(variables)


    def calculate_rate(self, current_status_value, mode=INCREMENT):
        status_rate = {}
        difference = 0
        for key, value in current_status_value.iteritems():
            if mode is INCREMENT:
                difference = value - self._pre_status_value[key]
            elif mode is FULLDOSE:
                difference = value
                self._pre_status_value[key] = 0
            rate = difference/float(self._interval)
            status_rate[key] = rate
        if mode is INCREMENT:
            self._pre_status_value = current_status_value
        return status_rate

    def calculate_items(self, current_status_value):
        status_items = {}

        #Key
        key_buff_size = long(self._global_variables['key_buffer_size'])
        key_blocks = current_status_value['Key_blocks_used'] + current_status_value['Key_blocks_unused']
        key_reads_hit = 100.0 - float(current_status_value['Key_reads'])/current_status_value['Key_read_requests']*100.0
        key_writes_hit = 100.0 - float(current_status_value['Key_writes'])/current_status_value['Key_write_requests']*100.0
        key_blocks_used_ratio = current_status_value['Key_blocks_used']/float(key_blocks)*100.0
        key_buff_usage  = key_buff_size - (current_status_value['Key_blocks_unused'] * long(self._global_variables['key_cache_block_size']))
        key_buff_used = current_status_value['Key_blocks_used'] * long(self._global_variables['key_cache_block_size'])
        key_buff_used_ratio = 100.0*key_buff_used/key_buff_size
        key_buf_usage_ratio = 100.0*key_buff_usage/key_buff_size

        status_items['Key_blocks'] = self.converse_1000(key_blocks, 1024)
        status_items['Key_blocks_used'] = key_blocks_used_ratio
        status_items['Key_reads_hit'] = key_reads_hit
        status_items['Key_writes_hit'] = key_writes_hit
        status_items['Key_buff_used'] = self.converse_1000(key_buff_used, 1024)
        status_items['Key_buff_usage'] = self.converse_1000(key_buff_usage, 1024)
        status_items['Key_buff_size'] = self.converse_1000(key_buff_size, 1024)
        status_items['Key_buff_usage_ratio'] = key_buf_usage_ratio
        status_items['Key_buff_used_ratio']  = key_buff_used_ratio


        #Questions
        dms_count = current_status_value['Com_insert'] + current_status_value['Com_select'] + \
                    current_status_value['Com_update'] + current_status_value['Com_delete'] + \
                    current_status_value['Com_replace']
        dms_ratio = float(dms_count)/current_status_value['Queries']*100.0
        pre_dms_count = self._pre_status_value['Com_insert'] + self._pre_status_value['Com_select'] + \
                        self._pre_status_value['Com_update'] + self._pre_status_value['Com_delete'] + \
                        self._pre_status_value['Com_replace']
        dms_rate = (dms_count - pre_dms_count)/float(self._interval)
        status_items['DMS_count'] = self.converse_1000(dms_count, 1000)
        status_items['DMS_ratio'] = dms_ratio
        status_items['DMS_rate'] = dms_rate

        status_items['log_slow_queries'] = self._global_variables['log_slow_queries']
        slow_query_dms_ratio = 100.0*current_status_value['Slow_queries']/dms_count
        slow_query_ratio = 100.0*current_status_value['Slow_queries']/current_status_value['Queries']
        status_items['Slow_queries_dms_ratio'] = slow_query_dms_ratio
        status_items['Slow_queries_ratio'] = slow_query_ratio

        #Com_
        com_count = 0
        for key, v in current_status_value.iteritems():
            if key.startswith('Com_'):
                if key not in ['Com_select', 'Com_insert','Com_insert_select','Com_replace','Com_replace_select',
                               'Com_update','Com_update_multi','Com_delete','Com_delete_multi']:
                    com_count += v
        pre_com_count = 0
        for key, v in self._pre_status_value.iteritems():
            if key.startswith('Com_'):
                if key not in ['Com_select', 'Com_insert','Com_insert_select','Com_replace','Com_replace_select',
                               'Com_update','Com_update_multi','Com_delete','Com_delete_multi']:
                    pre_com_count += v
        query_count = current_status_value['Queries']
        status_items['Com_count'] = self.converse_1000(com_count, 1000)
        com_rate = (com_count - pre_com_count)/float(self._interval)
        status_items['Com_rate'] = com_rate
        #TPS
        tps = current_status_value['Com_commit'] + current_status_value['Com_rollback']
        pre_tps = self._pre_status_value['Com_commit'] + self._pre_status_value['Com_rollback']
        tps_rate = (tps - pre_tps)/float(self._interval)
        status_items['TPS'] = tps_rate


        com_quit = int(current_status_value['Connections'] - 2 - (current_status_value['Aborted_clients']/2))
        com_quit_ratio = float(com_quit)/current_status_value['Queries']*100.0
        pre_com_quit = self._pre_status_value['Connections'] - 2 -(self._pre_status_value['Aborted_clients']/2)
        com_quit_rate = (com_quit - pre_com_quit)/float(self._interval)
        unknow = abs(current_status_value['Queries'] - com_quit - com_count - dms_count - current_status_value['Qcache_hits'])
        unknow_ratio = float(unknow)/current_status_value['Queries']*100.0
        pre_unknow = self._pre_status_value['Queries'] - pre_com_quit - pre_com_count  -pre_dms_count - self._pre_status_value['Qcache_hits']
        unknow_rate = (unknow - pre_unknow)/float(self._interval)
        status_items['COM_QUIT_count'] = self.converse_1000(com_quit, 1024)
        status_items['COM_QUIT_rate'] = com_quit_rate
        status_items['COM_QUIT_ratio'] = com_quit_ratio
        status_items['Unknown_count'] = self.converse_1000(unknow, 1024)
        status_items['Unknown_rate'] = unknow_rate
        status_items['Unknown_ratio'] = unknow_ratio
        status_items['Qcache_hits'] = 100.0*current_status_value['Qcache_hits']/query_count


        #Com_
        com_ratio = float(com_count)/query_count*100.0
        setoption_query_ratio = current_status_value['Com_set_option']/float(query_count)*100.0
        commit_query_ratio = current_status_value['Com_commit']/float(query_count)*100.0
        showcreate_query_ratio = current_status_value['Com_show_create_table']/float(query_count)*100.0
        status_items['Com_ratio'] = com_ratio
        status_items['Com_set_option'] = setoption_query_ratio
        status_items['Com_commit'] = commit_query_ratio
        status_items['Com_show_create_table'] = showcreate_query_ratio

        #DMS
        replace = current_status_value['Com_replace'] + current_status_value['Com_replace_select']
        update = current_status_value['Com_update'] + current_status_value['Com_update_multi']
        delete = current_status_value['Com_delete'] + current_status_value['Com_delete_multi']
        insert = current_status_value['Com_insert'] + current_status_value['Com_insert_select']
        replace_rate = float(replace)/self._interval
        update_rate = float(update)/self._interval
        delete_rate = float(delete)/self._interval
        insert_rate = float(insert)/self._interval
        status_items['replace'] = self.converse_1000(replace, 1000)
        status_items['update'] = self.converse_1000(update, 1000)
        status_items['delete'] = self.converse_1000(delete, 1000)
        status_items['insert'] = self.converse_1000(insert, 1000)
        status_items['replace_rate'] = replace_rate
        status_items['update_rate'] = update_rate
        status_items['delete_rate'] = delete_rate
        status_items['insert_rate'] = insert_rate


        insert_dms_ratio = insert/float(dms_count)*100.0
        select_dms_ratio = current_status_value['Com_select']/float(dms_count)*100.0
        update_dms_ratio = update/float(dms_count)*100.0
        delete_dms_ratio = delete/float(dms_count)*100.0
        replace_dms_ratio = replace/float(dms_count)*100.0
        status_items['Com_insert_dms'] = insert_dms_ratio
        status_items['Com_select_dms'] = select_dms_ratio
        status_items['Com_update_dms'] = update_dms_ratio
        status_items['Com_delete_dms'] = delete_dms_ratio
        status_items['Com_replace_dms'] = replace_dms_ratio

        insert_query_ratio = insert/float(query_count)*100.0
        select_query_ratio = current_status_value['Com_select']/float(query_count)*100.0
        update_query_ratio = update/float(query_count)*100.0
        delete_query_ratio = delete/float(query_count)*100.0
        replace_query_ratio = replace/float(query_count)*100.0
        status_items['Com_insert'] = insert_query_ratio
        status_items['Com_select'] = select_query_ratio
        status_items['Com_update'] = update_query_ratio
        status_items['Com_delete'] = delete_query_ratio
        status_items['Com_replace'] = replace_query_ratio

        #Table Locks
        table_locks_num = current_status_value['Table_locks_waited'] + current_status_value['Table_locks_immediate']
        table_locks_waited_ratio = current_status_value['Table_locks_waited']/float(table_locks_num)*100.0
        status_items['Table_locks_waited_ratio'] = table_locks_waited_ratio



        #Tables
        table_cache = self._global_variables['table_cache']
        table_cache_ratio = float(current_status_value['Open_tables'])/long(table_cache)*100.0
        status_items['table_cache'] = table_cache
        status_items['table_cache_ratio'] = table_cache_ratio

        #Connections
        max_connections = self._global_variables['max_connections']
        max_used_connections_ratio = float(current_status_value['Max_used_connections'])/long(max_connections)*100.0
        status_items['max_connections'] = max_connections
        status_items['max_used_connections_ratio'] = max_used_connections_ratio

        #Threads
        thread_hits = 100.0 - 100.0*current_status_value['Threads_created']/current_status_value['Connections']
        status_items['thread_hits'] = thread_hits


        #Select and Sort
        select_count = current_status_value['Select_scan'] + current_status_value['Select_range'] +\
                       current_status_value['Select_full_join'] + current_status_value['Select_range_check'] + \
                       current_status_value['Select_full_range_join']
        scan_select_ratio = current_status_value['Select_scan']/float(select_count)*100.0
        range_select_ratio = current_status_value['Select_range']/float(select_count)*100.0
        full_join_select_ratio = current_status_value['Select_full_join']/float(select_count)*100.0
        range_check_select_ratio = current_status_value['Select_range_check']/float(select_count)*100.0
        full_range_join_select_ratio = current_status_value['Select_full_range_join']/float(select_count)*100.0
        status_items['Select_scan'] = scan_select_ratio
        status_items['Select_range'] = range_select_ratio
        status_items['Select_full_join'] = full_join_select_ratio
        status_items['Select_range_check'] = range_check_select_ratio
        status_items['Select_full_range_join'] = full_range_join_select_ratio



        #Tables %Cache
        qcache_used_memory = long(self._global_variables['query_cache_size']) - current_status_value['Qcache_free_memory']
        qcache_used_blocks = current_status_value['Qcache_total_blocks'] - current_status_value['Qcache_free_blocks']
        qcache_used_memory_ratio = float(qcache_used_memory)/long(self._global_variables['query_cache_size'])*100.0
        insert_div_prune = float(current_status_value['Qcache_inserts'])/current_status_value['Qcache_lowmem_prunes']
        hit_div_insert = float(current_status_value['Qcache_hits'])/current_status_value['Qcache_inserts']
        #pre_insert_div_prune = float(self._pre_status_value['Qcache_inserts'])/self._pre_status_value['Qcache_lowmem_prunes']
        hit_div_prune_rate = float(current_status_value['Qcache_inserts'] - current_status_value['Qcache_lowmem_prunes'])/self._interval
        query_cache_hits = 100.0*current_status_value['Qcache_hits']/(current_status_value['Qcache_hits'] + current_status_value['Qcache_inserts'])
        status_items['Qcache_used_memory'] = self.converse_1000(qcache_used_memory, 1024)
        status_items['Qcache_total_blocks'] = qcache_used_blocks
        status_items['Qcache_used_memory_ratio'] = qcache_used_memory_ratio
        status_items['Insert_div_Prune'] = insert_div_prune
        status_items['Insert_div_Prune_rate'] = hit_div_prune_rate
        status_items['Hit_div_Insert'] = hit_div_insert
        status_items['Query_cache_hits'] = query_cache_hits


        #%Max
        #connection_used_ratio = current_status_value['']

        #Tmp
        status_items['tmp_table_size'] = self.converse_1000(self._global_variables['tmp_table_size'], 1024)

        #Query cache
        thread_cache_size = self._global_variables['thread_cache_size']
        status_items['thread_cache_size'] = self.converse_1000(thread_cache_size, 1024)
        status_items['query_cache_size'] = self.converse_1000(self._global_variables['query_cache_size'], 1024)
        status_items['Qcache_free_blocks'] = 100.0*float(current_status_value['Qcache_free_blocks'])/current_status_value['Qcache_total_blocks']

        #InnoDB Buffer Pool
        pages_count = current_status_value['Innodb_buffer_pool_pages_free'] + \
                current_status_value['Innodb_buffer_pool_pages_data'] + \
                current_status_value['Innodb_buffer_pool_pages_misc']
        free_pages_ratio = current_status_value['Innodb_buffer_pool_pages_free']/float(pages_count)*100.0
        data_pages_ratio = current_status_value['Innodb_buffer_pool_pages_data']/float(pages_count)*100.0
        misc_pages_ratio = current_status_value['Innodb_buffer_pool_pages_misc']/float(pages_count)*100.0
        dirty_pages_ratio = current_status_value['Innodb_buffer_pool_pages_dirty']/float(pages_count)*100.0
        innodb_buffer_pool_used = long(self._global_variables['innodb_buffer_pool_size']) - current_status_value['Innodb_buffer_pool_pages_free']
        innodb_buffer_pool_used_ratio = float(innodb_buffer_pool_used)/long(self._global_variables['innodb_buffer_pool_size'])*100.0
        innodb_buffer_pool_read_requests_hit = 100.0 - 100.0*current_status_value['Innodb_buffer_pool_reads']/\
                                               current_status_value['Innodb_buffer_pool_read_requests']
        innodb_buffer_pool_reads = 100.0*current_status_value['Innodb_buffer_pool_reads']/current_status_value['Innodb_buffer_pool_read_requests']
        status_items['Innodb_buffer_pool_pages_free_ratio'] = free_pages_ratio
        status_items['Innodb_buffer_pool_pages_data_ratio'] = data_pages_ratio
        status_items['Innodb_buffer_pool_pages_misc_ratio'] = misc_pages_ratio
        status_items['innodb_buffer_pool_used'] = self.converse_1000(innodb_buffer_pool_used, 1024)
        status_items['innodb_buffer_pool_used_ratio'] = innodb_buffer_pool_used_ratio
        status_items['innodb_buffer_pool_size'] = self.converse_1000(self._global_variables['innodb_buffer_pool_size'], 1024)
        status_items['Innodb_buffer_pool_read_requests'] = innodb_buffer_pool_read_requests_hit
        status_items['Innodb_buffer_pool_dirty'] = dirty_pages_ratio
        status_items['innodb_buffer_pool_ratio'] = float(innodb_buffer_pool_used)/long(self._global_variables['innodb_buffer_pool_size'])*100.0
        status_items['innodb_buffer_pool_reads_ratio'] = innodb_buffer_pool_reads
        #status_items['innodb_buffer_pool_size'] =

        return status_items

    def converse_1000(self, _value, step):
        _value = long(_value)
        value = ''
        if _value < step:
            value = str(_value)
        elif _value < step*step:
            value = "%.2fK" %(_value/float(step))
        elif _value < step*step*step:
            value = "%.2fM" %(_value/float(step)/float(step))
        else:
            value = "%.2fG" %(_value/float(step)/step/step)
        return value




    def mysql_status_process(self, status, file_handler=None, mode=INCREMENT):
        _status = dict(status)
        _status_value = {}
        result = ''
        for key, value in _status.iteritems():
            if value.isdigit():
                v = long(value)
            else:
                continue
            _status_value[key] = v
            value = self.converse_1000(v, 1000)
            _status[key] = value

        if not self._pre_status_value:
            if mode == INCREMENT:
                self._pre_status_value = _status_value
                self._interval = 1
                return
            else:
                self._interval = _status_value['Uptime']
        else:
            self._interval = _status_value['Uptime'] - self._pre_status_value['Uptime']

        _state_rate = self.calculate_rate(_status_value, mode)
        _status_items = self.calculate_items(_status_value)
        now_time = datetime.datetime.now().strftime("%Y%m%d-%H:%M:%S")

        result += "Current Time: %s\n" %now_time
        result += self.__key()
        result += '\n'
        result += self.__questions()
        result += '\n'
        result += self.__select_and_sort()
        result += '\n'

        if self._global_variables['query_cache_type'] == "ON":
            result += self.__query_cache()
            result += '\n'
        result += self.__table_locks()
        result += '\n'
        result += self.__tables()
        result += '\n'
        result += self.__connections()
        result += '\n'
        result += self.__created_temp()
        result += '\n'
        result += self.__threads()
        result += '\n'
        result += self.__aborted()
        result += '\n'
        result += self.__bytes_()
        result += '\n'
        result += self.__innodb_buffer_pool()
        result += '\n'
        result += self.__innodb_lock()
        result += '\n'
        result += self.__data()
        result += '\n'
        result += self.__pages()
        result += '\n'
        result += self.__rows()
        result += '\n'

        result = result %_status
        result = result %_state_rate
        result = result %_status_items
        if file_handler:
            file_handler.write(result)
        else:
            print(result)

    def __key(self):
        key = "__ Key _________________________________________________________________\n" \
              "Buffer used    %%%%(Key_buff_used)10s of  %%%%(Key_buff_size)10s  %%%%%%%%Used:  %%%%(Key_buff_used_ratio)4.2f\n" \
              "  Current    %%%%(Key_buff_usage)10s   %%%%%%%%Usage:  %%%%(Key_buff_usage_ratio)4.2f\n" \
              "Write hit    %%%%(Key_writes_hit)4.2f%%%%%%%%\n" \
              "Read hit     %%%%(Key_reads_hit)4.2f%%%%%%%%\n"
        return key

    def __questions(self):
        #QPS Queries / Seconds
        #TPS (Com_commit + Com_rollback) / Seconds
        questions = "__ Questions ___________________________________________________________\n" \
                    "Total      %(Queries)10s %%(Queries)10.2f/s(QPS)\n" \
                    "  DMS        %%%%(DMS_count)10s    %%%%(DMS_rate)10.2f/s  %%%%%%%%Total:  %%%%(DMS_ratio)6.2f\n" \
                    "  Com_       %%%%(Com_count)10s    %%%%(Com_rate)10.2f/s           %%%%(Com_ratio)6.2f\n" \
                    "  QC Hits    %(Qcache_hits)10s    %%(Qcache_hits)10.2f/s           %%%%(Qcache_hits)6.2f\n" \
                    "  COM_QUIT   %%%%(COM_QUIT_count)10s    %%%%(COM_QUIT_rate)10.2f/s           %%%%(COM_QUIT_ratio)6.2f\n" \
                    "  -Unknown   %%%%(Unknown_count)10s    %%%%(Unknown_rate)10.2f/s           %%%%(Unknown_ratio)6.2f\n" \
                    "Slow 10 s    %(Slow_queries)10s    %%(Slow_queries)10.2f/s           %%%%(Slow_queries_ratio)6.2f    %%%%%%%%DMS:   0.00  Log: %%%%(Slow_queries_dms_ratio)4.2f\n" \
                    "DMS          %%%%(DMS_count)10s    %%%%(DMS_rate)10.2f/s    %%%%(DMS_ratio)10.2f\n" \
                    "  INSERT     %%%%(insert)10s    %%%%(insert_rate)10.2f/s    %%%%(Com_insert)10.2f    %%%%(Com_insert_dms)10.2f\n" \
                    "  SELECT     %(Com_select)10s    %%%%(Com_select)10.2f/s    %%%%(Com_select)10.2f    %%%%(Com_select_dms)10.2f\n" \
                    "  UPDATE     %%%%(update)10s    %%%%(update_rate)10.2f/s    %%%%(Com_update)10.2f    %%%%(Com_update_dms)10.2f\n" \
                    "  DELETE     %%%%(delete)10s    %%%%(delete_rate)10.2f/s    %%%%(Com_delete)10.2f    %%%%(Com_delete_dms)10.2f\n" \
                    "  REPLACE    %%%%(replace)10s    %%%%(replace_rate)10.2f/s    %%%%(Com_replace)10.2f    %%%%(Com_replace_dms)10.2f\n" \
                    "Com_            %%%%(Com_count)10s    %%%%(Com_rate)10.2f/s    %%%%(Com_ratio)8.2f\n" \
                    "  set_option    %(Com_set_option)10s    %%(Com_set_option)10.2f/s    %%%%(Com_set_option)10.2f\n" \
                    "  commit        %(Com_commit)10s    %%(Com_commit)10.2f/s    %%%%(Com_commit)10.2f\n" \
                    "  show_create   %(Com_show_create_table)10s    %%(Com_show_create_table)10.2f/s    %%%%(Com_show_create_table)10.2f\n" \
                    "TPS           %%%%(TPS)10.2f/s\n"
        return questions

    def __select_and_sort(self):
        select_and_sort = "__ SELECT and Sort _____________________________________________________\n" \
                          "Scan           %(Select_scan)8s    %%(Select_scan)10.2f/s  %%%%%%%%SELECT: %%%%(Select_scan)4.2f\n" \
                          "Range          %(Select_range)8s    %%(Select_range)10.2f/s      %%%%(Select_range)10.2f\n" \
                          "Full join      %(Select_full_join)8s    %%(Select_full_join)10.2f/s      %%%%(Select_full_join)10.2f\n" \
                          "Range check    %(Select_range_check)8s    %%(Select_range_check)10.2f/s      %%%%(Select_range_check)10.2f\n" \
                          "Full rng join  %(Select_full_range_join)8s    %%(Select_full_range_join)10.2f/s      %%%%(Select_full_range_join)10.2f\n" \
                          "Sort scan      %(Sort_scan)8s    %%(Sort_scan)10.2f/s\n" \
                          "Sort range     %(Sort_range)8s    %%(Sort_range)10.2f/s\n" \
                          "Sort mrg pass  %(Sort_merge_passes)8s    %%(Sort_merge_passes)10.2f/s\n"
        return select_and_sort

    def __query_cache(self):
        query_cache = "__ Query Cache _________________________________________________________\n" \
                      "Memory usage   %%%%(Qcache_used_memory)8s of  %%%%(query_cache_size)8s  %%%%%%%%Used:  %%%%(Qcache_used_memory_ratio)4.2f\n" \
                      "Block Fragmnt  %%%%(Qcache_free_blocks)6.2f%%%%%%%%\n" \
                      "Hit            %%%%(Query_cache_hits)6.2f%%%%%%%%\n" \
                      "Hits           %(Qcache_hits)10s    %%(Qcache_hits)10.2f/s\n" \
                      "Inserts        %(Qcache_inserts)10s    %%(Qcache_inserts)10.2f/s\n" \
                      "Insrt:Prune   %%%%(Insert_div_Prune)10.2f:1    %%%%(Insert_div_Prune_rate)10.2f/s\n" \
                      "Hit:Insert    %%%%(Hit_div_Insert)10.2f:1\n"
        return query_cache


    def __table_locks(self):
        table_locks = "__ Table Locks _________________________________________________________\n" \
                      "Waited       %(Table_locks_waited)10s    %%(Table_locks_waited)10.2f/s  %%%%%%%%Total:  %%%%(Table_locks_waited_ratio)10.2f\n" \
                      "Immediate    %(Table_locks_immediate)10s    %%(Table_locks_immediate)10.2f/s\n"
        return table_locks

    def __tables(self):
        tables = "__ Tables ______________________________________________________________\n" \
                 "Open      %(Open_tables)10s  of  %%%%(table_cache)10s   %%%%%%%%Cache:%%%%(table_cache_ratio)10.2f\n" \
                 "Opened    %(Opened_tables)10s    %%(Opened_tables)10.2f/s\n"
        return tables

    def __connections(self):
        connections = "__ Connections _________________________________________________________\n" \
                      "Max used       %(Max_used_connections)10s of  %%%%(max_connections)10s      %%%%%%%%Max:  %%%%(max_used_connections_ratio)10.2f\n" \
                      "Total          %(Connections)10s    %%(Connections)10.2f/s\n"
        return connections

    def __created_temp(self):
        created_temp = "__ Created Temp ________________________________________________________\n" \
                       "Disk table    %(Created_tmp_disk_tables)10s    %%(Created_tmp_disk_tables)10.2f/s\n" \
                       "Table         %(Created_tmp_tables)10s    %%(Created_tmp_tables)10.2f/s    Size:  %%%%(tmp_table_size)10s\n" \
                       "File          %(Created_tmp_files)10s    %%(Created_tmp_files)10.2f/s\n"
        return created_temp

    def __threads(self):
        threads = "__ Threads _____________________________________________________________\n" \
                  "Running        %(Threads_running)10s  of  %(Threads_connected)6s\n" \
                  "Cached         %(Threads_cached)10s  of  %%%%(thread_cache_size)6s      %%%%%%%%Hit:  %%%%(thread_hits)4.2f\n" \
                  "Created        %(Threads_created)10s    %%(Threads_created)10.2f/s\n" \
                  "Slow           %(Slow_launch_threads)10s    %%(Slow_launch_threads)10.2f/s\n"
        return threads


    def __aborted(self):
        aborted = "__ Aborted _____________________________________________________________\n" \
                  "Clients     %(Aborted_clients)10s    %%(Aborted_clients)10.2f/s\n" \
                  "Connects    %(Aborted_connects)10s    %%(Aborted_connects)10.2f/s\n"
        return aborted

    def __bytes_(self):
        bytes_ = "__ Bytes _______________________________________________________________\n" \
                "Sent         %(Bytes_sent)10s    %%(Bytes_sent)10.2f/s\n" \
                "Received     %(Bytes_received)10s    %%(Bytes_received)10.2f/s\n"
        return bytes_

    def __innodb_buffer_pool(self):
        "Delete Latched"
        innodb_buffer_pool = "__ InnoDB Buffer Pool __________________________________________________\n" \
                             "Pages\n" \
                             "Usage      %%%%(innodb_buffer_pool_used)10s  of  %%%%(innodb_buffer_pool_size)10s  %%%%%%%%Used: %%%%(innodb_buffer_pool_ratio)4.2f\n" \
                             "Read hit   %%%%(Innodb_buffer_pool_read_requests)4.2f%%%%%%%%\n" \
                             "  Free       %(Innodb_buffer_pool_pages_free)10s    %%%%%%%%Total:  %%%%(Innodb_buffer_pool_pages_free_ratio)4.2f\n" \
                             "  Data       %(Innodb_buffer_pool_pages_data)10s            %%%%(Innodb_buffer_pool_pages_data_ratio)4.2f   %%%%%%%%Drty:  %%%%(Innodb_buffer_pool_dirty)4.2f\n" \
                             "  Misc       %(Innodb_buffer_pool_pages_misc)10s             %%%%(Innodb_buffer_pool_pages_misc_ratio)4.2f\n" \
                             "Reads        %(Innodb_buffer_pool_read_requests)10s   %%(Innodb_buffer_pool_read_requests)10.2f/s\n" \
                             "  From file  %(Innodb_buffer_pool_reads)10s    %%(Innodb_buffer_pool_reads)10.2f/s           %%%%(innodb_buffer_pool_reads_ratio)4.2f\n" \
                             "  Ahead Rnd  %(Innodb_buffer_pool_read_ahead_rnd)10s    %%(Innodb_buffer_pool_read_ahead_rnd)10.2f/s\n" \
                             "  Ahead Sql  %(Innodb_buffer_pool_read_ahead_seq)10s    %%(Innodb_buffer_pool_read_ahead_seq)10.2f/s\n" \
                             "Writes       %(Innodb_buffer_pool_write_requests)10s    %%(Innodb_buffer_pool_write_requests)10.2f/s\n" \
                             "Flushes      %(Innodb_buffer_pool_pages_flushed)10s    %%(Innodb_buffer_pool_pages_flushed)10.2f/s\n" \
                             "Wait Free    %(Innodb_buffer_pool_wait_free)10s    %%(Innodb_buffer_pool_wait_free)10.2f/s\n"
        return innodb_buffer_pool


    def __innodb_lock(self):
        innodb_lock = "__ InnoDB Lock _________________________________________________________\n" \
                      "Waits           %(Innodb_row_lock_waits)10s    %%(Innodb_row_lock_waits)10.2f/s\n" \
                      "Current         %(Innodb_row_lock_current_waits)10s\n" \
                      "Time acquiring\n" \
                      "  Total          %(Innodb_row_lock_time)10s ms\n" \
                      "  Average        %(Innodb_row_lock_time_avg)10s ms\n" \
                      "  Max            %(Innodb_row_lock_time_max)10s ms\n"
        return innodb_lock


    def __data(self):
        data = "Data\n" \
               "  Reads   %(Innodb_data_reads)10s      %%(Innodb_data_reads)10.2f/s\n" \
               "  Writes  %(Innodb_data_writes)10s      %%(Innodb_data_writes)10.2f/s\n" \
               "  fsync   %(Innodb_data_fsyncs)10s      %%(Innodb_data_fsyncs)10.2f/s\n" \
               "  Pending\n" \
               "    Reads   %(Innodb_data_pending_reads)10s\n" \
               "    Writes  %(Innodb_data_pending_writes)10s\n" \
               "    fsync   %(Innodb_data_pending_fsyncs)10s\n"
        return data

    def __pages(self):
        pages = "Pages\n" \
                "  Created  %(Innodb_pages_created)10s    %%(Innodb_pages_created)10.2f/s\n" \
                "  Read     %(Innodb_pages_read)10s    %%(Innodb_pages_read)10.2f/s\n" \
                "  Written  %(Innodb_pages_written)10s    %%(Innodb_pages_written)10.2f/s\n"
        return pages

    def __rows(self):
        rows = "Rows\n" \
               "  Deleted   %(Innodb_rows_deleted)10s    %%(Innodb_rows_deleted)10.2f/s\n" \
               "  Inserted  %(Innodb_rows_inserted)10s    %%(Innodb_rows_inserted)10.2f/s\n" \
               "  Read      %(Innodb_rows_read)10s    %%(Innodb_rows_read)10.2f/s\n" \
               "  Upated    %(Innodb_rows_updated)10s    %%(Innodb_rows_updated)10.2f/s\n"
        return rows

def usage():
    print("Usage: python SendPayload.py  -h [host] -u [user] -p [password] [ -P port]")
    print("--help               Print usage")
    print("-h <host>            Host of the mysql server")
    print("-u <user>            username")
    print("-p <password>        password")
    print("-P <port>            Default set to 3306")
    print("-t <interval>        Interval between queries")
    print("-l <loop>            Loop through the query X times")
    print("-f <filename>        Save the query results to the file or print to screen")
    print("-m <mode>            Value 1: increment values between two queries"
          "                            2: full values from the mysql start."
          "                            Default is 2")

#Entrance
def start_send():
    try:
        options, args = getopt.getopt(sys.argv[1:], "h:u:p:P:t:l:f:m:",
            ["help", "host=", "user=", "password=", "port=","interval=", "loop=", "file="])
        #options, args = getopt.getopt(sys.argv[1:], "hp:i:", ["help", "ip=", "port="])
    except getopt.GetoptError as e:
        print(e.message)
        sys.exit(0)
    else:
        host = ''
        user = ''
        password = ''
        port = 3306
        interval = 5
        count = 1
        mode = 2
        filename = None
        for name, value in options:
            if name is "--help":
                usage()
                exit(0)
            if name == '-h':
                host = value
            if name == '-u':
                user = value
            if name == '-p':
                password = value
            if name == '-P':
                port = int(value)
            if name == "-l":
                count = int(value)
            if name == "-t":
                interval = int(value)
            if name == "-f":
                filename = value
            if name == "-m":
                mode = int(value)
        if host and user and password:
            mysqlreport = MySQLGlobal("Mysqlreport", host=host, user=user, password=password, port=port,
                                      interval= interval, count=count, filename=filename, mode=mode)
            mysqlreport.start()
        else:
            usage()
            sys.exit(0)

start_send()









