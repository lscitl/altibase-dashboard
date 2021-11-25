import jaydebeapi
import json


classname = 'Altibase.jdbc.driver.AltibaseDriver'
classfile = './Altibase.jar'
dbuser = 'sys'
passwd = 'manager'

conn, cursor = None, None

try :
    conn = jaydebeapi.connect (classname,
                                'jdbc:Altibase://10.51.25.2:20300/mydb',
                                {'user': dbuser, 'password': passwd},
                                classfile)

    cursor = conn.cursor()

    sql = "SELECT * FROM STANDARD_TEST_RESULT"
    cursor.execute(sql)

    data = cursor.fetchall()
    STR_index = ["PER_TEST_ID", "DATA_CATEGORY", "CONNECT_TYPE", "DATA_SCALE",
                "DATA_CONTROL", "THREAD", "INTERFACE", "TEST_RESULT_1", "TEST_RESULT_2",
                "TEST_RESULT_3", "TEST_RESULT_4", "TEST_RESULT_5", "TEST_RESULT_AVG",
                "TEST_ITEM", "TEST_START_TIME", "TEST_END_TIME", "CPU_USAGE_1",
                "CPU_USAGE_2", "CPU_USAGE_3", "CPU_USAGE_4", "CPU_USAGE_5", "CPU_USAGE_AVG",
                "MEMORY_USAGE_1", "MEMORY_USAGE_2", "MEMORY_USAGE_3", "MEMORY_USAGE_4",
                "MEMORY_USAGE_5", "MEMORY_USAGE_AVG", "NODE_ID", "LATENCY_1", "LATENCY_2",
                "LATENCY_3", "LATENCY_4", "LATENCY_5", "LATENCY_AVG", "MIN_1", "MIN_2", "MIN_3",
                "MIN_4", "MIN_5", "MIN_AVG", "MAX_1", "MAX_2", "MAX_3", "MAX_4", "MAX_5", "MAX_AVG",
                "AVG_1", "AVG_2", "AVG_3", "AVG_4", "AVG_5", "AVG_AVG"]
    
    STR_index_type_str = [1, 2, 4, 6, 13, 14, 15]
    STR_index_type_none = [28]
    # print(json.dumps(data))

    STR = []

    for test_num in data:
        STR_sub = []
        for index in range(len(test_num)):
            if index in STR_index_type_str:
                STR_sub.append({STR_index[index] : str(test_num[index]).strip()})
            elif index in STR_index_type_none:
                STR_sub.append({STR_index[index] : 'none'})
            else:
                STR_sub.append({STR_index[index] : float(test_num[index])})
        STR.append(STR_sub)

    DATA_BASE = dict()
    DATA_BASE["STANDARD_TEST_RESULT"] = STR

    # print(DATA_BASE["STANDARD_TEST_RESULT"][0])

    sql = "SELECT * FROM STANDARD_TEST_SUMMARY"
    cursor.execute(sql)

    data = cursor.fetchall()

    STS_index = ["PER_TEST_ID", "DATA_CATEGORY", "CONNECT_TYPE", "DATA_SCALE", "INTERFACE",
                "TEST_ITEM", "TEST_START_TIME", "TEST_END_TIME", "INSERT_AVG", "UPDATE_AVG",
                "SELECT_AVG", "DELETE_AVG", "INSERT_SELECT_AVG", "SELECT_UPDATE_AVG",
                "INSERT_SELECT_UPDATE_AVG", "COMMENT", "MODIFIER", "THREAD", "NODE_ID",
                "LATENCY_AVG", "MIN_AVG", "MAX_AVG", "AVG_AVG"]

    STS_index_type_str = [1, 2, 4, 5, 6, 7]
    STS_index_type_float = [12, 13, 14]
    STS_index_type_none = [15, 16, 18, 19, 20, 21, 22]

    STS = []
    for test_num in data:
        STS_sub = []
        for index in range(len(test_num)):
            if index in STS_index_type_str:
                STS_sub.append({STS_index[index] : str(test_num[index]).strip()})
            elif index in STS_index_type_float:
                if str(type(test_num[index])) == "<class 'NoneType'>":
                    STS_sub.append({STS_index[index] : float(0)})
                else:
                    STS_sub.append({STS_index[index] : float(test_num[index])})
            elif index in STS_index_type_none:
                STS_sub.append({STS_index[index] : 'none'})
            else:
                STS_sub.append({STS_index[index] : float(test_num[index])})
        STS.append(STS_sub)

    DATA_BASE["STANDARD_TEST_SUMMARY"] = STS

    sql = "SELECT * FROM PERFORMANCE_TEST_INFO"
    cursor.execute(sql)

    data = cursor.fetchall()

    PTI_index = ["ALTIBASE_URL", "ALTIBASE_REVISION", "LAST_UPDATE", "PER_TEST_ID", "TEST_TIMES",
                "OS", "HOST_NAME", "ACCOUNT", "ALTIBASE_VERSION", "JENKINS_BUILD_URL", "USER_NAME",
                "SOURCE_NAME", "TX_LEVEL", "CLIORNOT", "COMMITCT"]

    PTI_index_type_str = [0, 1, 2, 5, 6, 7, 8]
    PTI_index_type_float = [12, 14]
    # PTI_index_type_none = []

    PTI = []
    for test_num in data:
        PTI_sub = []
        for index in range(len(test_num)):
            # print(y)
            if index in PTI_index_type_str:
                PTI_sub.append({PTI_index[index] : str(test_num[index]).strip()})
            elif index in PTI_index_type_float:
                PTI_sub.append({PTI_index[index] : float(test_num[index])})
            else:
                PTI_sub.append({PTI_index[index] : 'none'})
                
        PTI.append(PTI_sub)

    DATA_BASE["PERFORMANCE_TEST_INFO"] = PTI

    print("==========DATA_BASE==========")
    for table in DATA_BASE:
        print("%s" %table)
        for test_num in DATA_BASE[table]:
            print(test_num)
            print('')
        print('\n')


    cursor.close()
    conn.close()

except Exception as msg:
      print ('Error Message : %s' %msg)
