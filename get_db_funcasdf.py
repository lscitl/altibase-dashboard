import jaydebeapi
import json

classname = 'Altibase.jdbc.driver.AltibaseDriver'
classfile = './Altibase.jar'
dbuser = 'sys'
passwd = 'manager'

conn, cursor = None, None

STR_index = ["PER_TEST_ID", "DATA_CATEGORY", "CONNECT_TYPE", "DATA_SCALE", "DATA_CONTROL",
            "THREAD", "INTERFACE", "TEST_RESULT_1", "TEST_RESULT_2", "TEST_RESULT_3",
            "TEST_RESULT_4", "TEST_RESULT_5", "TEST_RESULT_AVG", "TEST_ITEM", "TEST_START_TIME",
            "TEST_END_TIME", "CPU_USAGE_1", "CPU_USAGE_2", "CPU_USAGE_3", "CPU_USAGE_4",
            "CPU_USAGE_5", "CPU_USAGE_AVG", "MEMORY_USAGE_1", "MEMORY_USAGE_2", "MEMORY_USAGE_3",
            "MEMORY_USAGE_4", "MEMORY_USAGE_5", "MEMORY_USAGE_AVG", "NODE_ID", "LATENCY_1",
            "LATENCY_2", "LATENCY_3", "LATENCY_4", "LATENCY_5", "LATENCY_AVG",
            "MIN_1", "MIN_2", "MIN_3", "MIN_4", "MIN_5",
            "MIN_AVG", "MAX_1", "MAX_2", "MAX_3", "MAX_4",
            "MAX_5", "MAX_AVG", "AVG_1", "AVG_2", "AVG_3",
            "AVG_4", "AVG_5", "AVG_AVG"]

STS_index = ["PER_TEST_ID", "DATA_CATEGORY", "CONNECT_TYPE", "DATA_SCALE", "INTERFACE",
            "TEST_ITEM", "TEST_START_TIME", "TEST_END_TIME", "INSERT_AVG", "UPDATE_AVG",
            "SELECT_AVG", "DELETE_AVG", "INSERT_SELECT_AVG", "SELECT_UPDATE_AVG", "INSERT_SELECT_UPDATE_AVG",
            "COMMENT", "MODIFIER", "THREAD", "NODE_ID", "LATENCY_AVG",
            "MIN_AVG", "MAX_AVG", "AVG_AVG"]

PTI_index = ["ALTIBASE_URL", "ALTIBASE_REVISION", "LAST_UPDATE", "PER_TEST_ID", "TEST_TIMES",
            "OS", "HOST_NAME", "ACCOUNT", "ALTIBASE_VERSION", "JENKINS_BUILD_URL",
            "USER_NAME", "SOURCE_NAME", "TX_LEVEL", "CLIORNOT", "COMMITCT"]

index_dict = {'STANDARD_TEST_RESULT':STR_index, 'STANDARD_TEST_SUMMARY':STS_index, 'PERFORMANCE_TEST_INFO':PTI_index}

def get_db(table_name, **kwargs):
    try :
        conn = jaydebeapi.connect (classname,
                                    'jdbc:Altibase://10.51.25.2:20300/mydb',
                                    {'user': dbuser, 'password': passwd},
                                    classfile)
        cursor = conn.cursor()
        # print(TBL_index)

        if 'COLUMN' in kwargs:
            sql = 'SELECT ' + (', ').join(kwargs['COLUMN']).upper() + ' FROM ' + table_name.upper()
            TBL_index = kwargs['COLUMN']
        else:
            sql = 'SELECT * FROM ' + table_name.upper()
            TBL_index = index_dict[table_name.upper()]
            # print(TBL_index)
        if 'WHERE' in kwargs:
            sql = sql + ' WHERE ' + (' and ').join(kwargs['WHERE']).upper()

        # print(sql)
        cursor.execute(sql)
        data = cursor.fetchall()

        # print(data)
        
        TBL = []
        for test_num in data:
            TBL_sub = dict()
            for index in range(len(test_num)):
                TBL_sub[TBL_index[index]] = str(test_num[index]).strip()
                # if index in TBL_index_type_none:
                #     TBL_sub.append({TBL_index[index] : 'none'})
            TBL.append(TBL_sub)

        cursor.close()
        conn.close()

        print(TBL)
        return (TBL)

    except Exception as msg:
        print ('Error Message : %s' %msg)
    
# get_db("standard_test_summary", kwargs)
get_db("standard_test_result")

