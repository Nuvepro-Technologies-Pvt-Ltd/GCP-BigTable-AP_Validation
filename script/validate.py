from result_output import *
import sys
import json
import importlib.util
import urllib.request
from googleapiclient import discovery
from google.oauth2 import service_account
from pprint import pprint
from google.cloud import bigtable
from google.cloud import storage

class Activity():

    def testcase_check_BigTable_Instance_name(self,test_object,credentials,project_id):
        testcase_description="Check BigTable Instance name"
        expected_result='Company Sales'
        try:
            is_present = False
            actual = 'BigTable Instance name is not '+ expected_result
            try:
                client = storage.Client(credentials=credentials, project=project_id, admin=True)
                for instance_local in client.list_instances()[0]:
                    if instance_local.display_name == expected_result:
                        is_present=True
                        actual=expected_result
                        break
                    else:
                        actual=instance_local.display_name
            except Exception as e:
                is_present = False
            test_object.update_pre_result(testcase_description,expected_result)
            if is_present==True:
                test_object.update_result(1,expected_result,actual,"No Comment"," Congrats! You have done it right!") 
            else:
                test_object.update_result(0,expected_result,actual,"Check instance name","https://cloud.google.com/bigtable/docs/")   

        except Exception as e:    
            test_object.update_result(-1,expected_result,"Internal Server error","Please check with Admin","")
            test_object.eval_message["testcase_check_ServiceAccount_name"]=str(e)                

    def testcase_check_Table_name(self,test_object,credentials,project_id):
        testcase_description="Check BigTable Table name"
        bigtable_id = "Company Sales"
        expected_result='UserSessions'
        try:
            is_present = False
            actual = 'Table name is not '+ expected_result
            try:
                client = storage.Client(credentials=credentials, project=project_id, admin=True)
                for instance in client.list_instances()[0]:
                    if instance.display_name == bigtable_id:
                        tables = instance.list_tables()
                        if tables != []:
                            for tbl in tables:
                                if tbl.table_id == expected_result:
                                    is_present=True
                                    actual=expected_result
                                    break
                    else:
                        actual= 'Table name is not '+ expected_result
                    if is_present:
                        break
            except Exception as e:
                is_present = False
            test_object.update_pre_result(testcase_description,expected_result)
            
            if is_present==True:
                test_object.update_result(1,expected_result,actual,"No Comment"," Congrats! You have done it right!") 
            else:
                test_object.update_result(0,expected_result,actual,"Check instance name","https://cloud.google.com/bigtable/docs/")   

        except Exception as e:    
            test_object.update_result(-1,expected_result,"Internal Server error","Please check with Admin","")
            test_object.eval_message["testcase_check_ServiceAccount_name"]=str(e)                

    def testcase_check_column_family1(self,test_object,credentials,project_id):
        testcase_description="Check Column Family name"
        bigtable_id = "Company Sales"
        bigtable_table_id = "UserSessions"
        expected_result='Instances'
        try:
            is_present = False
            actual = 'Column Family names is not '+ expected_result
            try:
                client = bigtable.Client(credentials=credentials, project=project_id, admin=True)
                for instance in client.list_instances()[0]:
                    if instance.display_name == bigtable_id:
                        tables = instance.list_tables()
                        if tables != []:
                            for tbl in tables:
                                if tbl.table_id == bigtable_table_id:
                                    table = instance.table(tbl.table_id)
                                    column_families = table.list_column_families()
                                    print(column_families)
                                    for column_family in column_families:
                                        if column_family == expected_result:
                                            print(column_family)
                                            is_present=True
                                            actual=expected_result
                                        break
                            if is_present:
                                break
                    else:
                        actual= 'Table name is not '+ expected_result
                    if is_present:
                        break
            except Exception as e:
                is_present = False
            test_object.update_pre_result(testcase_description,expected_result)
            
            if is_present==True:
                test_object.update_result(1,expected_result,actual,"No Comment"," Congrats! You have done it right!") 
            else:
                test_object.update_result(0,expected_result,actual,"Check instance name","https://cloud.google.com/bigtable/docs/")   

        except Exception as e:    
            test_object.update_result(-1,expected_result,"Internal Server error","Please check with Admin","")
            test_object.eval_message["testcase_check_ServiceAccount_name"]=str(e)                

    def testcase_check_column_family2(self,test_object,credentials,project_id):
        testcase_description="Check Column Family name"
        bigtable_id = "Company Sales"
        bigtable_table_id = "UserSessions"
        expected_result='Sales'
        try:
            is_present = False
            actual = 'Column Family names is not '+ expected_result
            try:
                client = bigtable.Client(credentials=credentials, project=project_id, admin=True)
                for instance in client.list_instances()[0]:
                    if instance.display_name == bigtable_id:
                        tables = instance.list_tables()
                        if tables != []:
                            for tbl in tables:
                                if tbl.table_id == bigtable_table_id:
                                    table = instance.table(tbl.table_id)
                                    column_families = table.list_column_families()
                                    print(column_families)
                                    for column_family in column_families:
                                        if column_family == expected_result:
                                            print(column_family)
                                            is_present=True
                                            actual=expected_result
                                        break
                            if is_present:
                                break
                    else:
                        actual= 'Table name is not '+ expected_result
                    if is_present:
                        break
            except Exception as e:
                is_present = False
            test_object.update_pre_result(testcase_description,expected_result)
            
            if is_present==True:
                test_object.update_result(1,expected_result,actual,"No Comment"," Congrats! You have done it right!") 
            else:
                test_object.update_result(0,expected_result,actual,"Check instance name","https://cloud.google.com/bigtable/docs/")   

        except Exception as e:    
            test_object.update_result(-1,expected_result,"Internal Server error","Please check with Admin","")
            test_object.eval_message["testcase_check_ServiceAccount_name"]=str(e)                

    def testcase_check_Storage_Bucket_name(self,test_object,credentials,project_id):
        testcase_description="Check Storage Bucket name"
        expected_result='my-bucket-bigtable'
        try:
            is_present = False
            actual = 'Storage Bucket name is not '+ expected_result
            try:
                #client = storage.Client()
                client = storage.Client(credentials=credentials, project=project_id)
                buckets = client.list_buckets()

                for bucket in buckets:
                    if bucket.name == expected_result:
                        is_present=True
                        actual=expected_result
                        break
                    else:
                        actual=bucket.name
            except Exception as e:
                is_present = False
            test_object.update_pre_result(testcase_description,expected_result)
            if is_present==True:
                test_object.update_result(1,expected_result,actual,"No Comment"," Congrats! You have done it right!") 
            else:
                test_object.update_result(0,expected_result,actual,"Check instance name","https://cloud.google.com/bigtable/docs/")   

        except Exception as e:    
            test_object.update_result(-1,expected_result,"Internal Server error","Please check with Admin","")
            test_object.eval_message["testcase_check_ServiceAccount_name"]=str(e)                

def start_tests(credentials, project_id, args):

    if "result_output" not in sys.modules:
        importlib.import_module("result_output")
    else:
        importlib.reload(sys.modules[ "result_output"])
    
    test_object=ResultOutput(args,Activity)
    challenge_test=Activity()
    challenge_test.testcase_check_BigTable_Instance_name(test_object,credentials,project_id)
    challenge_test.testcase_check_Table_name(test_object,credentials,project_id)
    challenge_test.testcase_check_column_family1(test_object,credentials,project_id)
    challenge_test.testcase_check_column_family2(test_object,credentials,project_id)
    challenge_test.testcase_check_Storage_Bucket_name(test_object,credentials,project_id)

    json.dumps(test_object.result_final(),indent=4)
    return test_object.result_final()

