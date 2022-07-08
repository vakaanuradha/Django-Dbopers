from django.shortcuts import render

# Create your views here.
from django.http import JsonResponse
from .database import Database
from django.views.decorators.csrf import csrf_exempt
import mysql.connector as conn
import json
import csv


class MysqlDatabase(Database):

    def health_check(self, request):
        return JsonResponse('Success', safe=False)

    def mysql_connection(self,request):
        host_name = request.POST.get('host')
        user_name = request.POST.get('username')
        password = request.POST.get('password')
        schema_name = request.POST.get('schema_name')
        mysql_conn = conn.connect(host=host_name, user=user_name, passwd=password, use_pure=True, database=schema_name)
        return mysql_conn

    @csrf_exempt
    def create_schema(self, request):
        try:
            host_name = request.POST.get('host')
            user_name = request.POST.get('username')
            password = request.POST.get('password')
            schema_name = request.POST.get('schema_name')
            mysql_conn = conn.connect(host=host_name, user=user_name, passwd=password, use_pure=True)
            cursor = mysql_conn.cursor()
            query = f"CREATE DATABASE {schema_name}"
            cursor.execute(query)
            mysql_conn.close()
            return JsonResponse(f"Schema {schema_name} created successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def drop_schema(self, request):
        try:
            host_name = request.POST.get('host')
            user_name = request.POST.get('username')
            password = request.POST.get('password')
            schema_name = request.POST.get('schema_name')
            mysql_conn = conn.connect(host=host_name, user=user_name, passwd=password, use_pure=True)
            cursor = mysql_conn.cursor()
            query = f"DROP DATABASE IF EXISTS {schema_name}"
            cursor.execute(query)
            mysql_conn.close()
            return JsonResponse(f"Schema {schema_name} dropped successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def create_table(self, request):
        try:
            table_name = request.POST.get('table_name')
            columns = request.POST.get('columns')
            columns = json.loads(columns)
            column_names = "(" + ", ".join(["{} {}".format(k, v) for k, v in columns.items()]) + ")"
            query = f"CREATE TABLE if not exists {table_name} {column_names}"
            mysql_conn = self.mysql_connection(request)
            cursor = mysql_conn.cursor()
            cursor.execute(query)
            mysql_conn.close()
            return JsonResponse(f"Table {table_name} created successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def drop_table(self, request):
        try:
            table_name = request.POST.get('table_name')
            query = f"DROP TABLE IF EXISTS {table_name}"
            mysql_conn = self.mysql_connection(request)
            cursor = mysql_conn.cursor()
            cursor.execute(query)
            mysql_conn.close()
            return JsonResponse(f"Table {table_name} deleted successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def insert_record(self, request):
        try:
            table_name = request.POST.get('table_name')
            columns = request.POST.get('columns')
            columns = json.loads(columns)
            column_names = "(" + ", ".join(columns.keys()) + ")"
            value_s = "(" + ", ".join(["%s" for key in columns.keys()]) + ")"
            query = f"INSERT INTO {table_name} {column_names} VALUES {value_s}"
            values = tuple(columns.values())
            mysql_conn = self.mysql_connection(request)
            cursor = mysql_conn.cursor()
            cursor.execute(query, values)
            mysql_conn.commit()
            mysql_conn.close()
            return JsonResponse(f"Data added successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def insert_multiple_records(self, request):
        try:
            table_name = request.POST.get('table_name')
            columns = request.POST.get('columns')
            columns = json.loads(columns.replace("'",'"'))
            column_names = "(" + ", ".join(columns) + ")"
            value_s = "(" + ", ".join(["%s" for key in columns]) + ")"
            input_file_name = request.POST.get('input_file_name')
            with open(input_file_name) as file:
                csv_reader = csv.reader(file)
                rows_list = [tuple(row) for row in csv_reader]
            query = f"INSERT INTO {table_name} {column_names} VALUES {value_s}"
            mysql_conn = self.mysql_connection(request)
            cursor = mysql_conn.cursor()
            cursor.executemany(query, rows_list)
            mysql_conn.commit()
            mysql_conn.close()
            return JsonResponse(f"Data added successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def update_records(self, request):
        try:
            table_name = request.POST.get('table_name')
            filters = request.POST.get('filters')
            filters = json.loads(filters)
            updates = request.POST.get('updated_values')
            updates = json.loads(updates)
            updated_values = ", ".join([f"{k} = '{v}'" for k, v in updates.items()])
            filter_values = " and ".join([f"{k} = '{v}'" for k, v in filters.items()])
            query = f"UPDATE {table_name} SET {updated_values} WHERE {filter_values}"
            mysql_conn = self.mysql_connection(request)
            cursor = mysql_conn.cursor()
            cursor.execute(query)
            mysql_conn.commit()
            mysql_conn.close()
            return JsonResponse(f"Data updated successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def retrieve_records(self, request):
        try:
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            mysql_conn = self.mysql_connection(request)
            cursor = mysql_conn.cursor()
            query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE " \
                    f"TABLE_SCHEMA= '{schema_name}' and TABLE_NAME = '{table_name}'"
            cursor.execute(query)
            columns = cursor.fetchall()
            query = f"SELECT *FROM {table_name}"
            cursor.execute(query)
            records = cursor.fetchall()
            mysql_conn.close()
            print(columns)
            output = [{columns[i][0]: rec[i] for i in range(len(rec))} for rec in records]
            return JsonResponse(output, safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def retrieve_records_with_filter(self, request):
        try:
            table_name = request.POST.get('table_name')
            filters = request.POST.get('filters')
            filters = json.loads(filters)
            filter_values = " and ".join([f"{k} = '{v}'" for k, v in filters.items()])
            query = f"SELECT *FROM {table_name} WHERE {filter_values}"
            mysql_conn = self.mysql_connection(request)
            cursor = mysql_conn.cursor()
            cursor.execute(query)
            records = cursor.fetchall()
            mysql_conn.close()
            return JsonResponse(records, safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def delete_records_with_filter(self, request):
        try:
            table_name = request.POST.get('table_name')
            filters = request.POST.get('filters')
            filters = json.loads(filters)
            filter_values = " and ".join([f"{k} = '{v}'" for k, v in filters.items()])
            query = f"DELETE FROM {table_name} WHERE {filter_values}"
            mysql_conn = self.mysql_connection(request)
            cursor = mysql_conn.cursor()
            cursor.execute(query)
            mysql_conn.commit()
            mysql_conn.close()
            return JsonResponse(f"Records deleted from {table_name}", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def delete_records(self, request):
        try:
            table_name = request.POST.get('table_name')
            query = f"DELETE FROM {table_name}"
            mysql_conn = self.mysql_connection(request)
            cursor = mysql_conn.cursor()
            cursor.execute(query)
            mysql_conn.commit()
            mysql_conn.close()
            return JsonResponse(f"Records deleted from {table_name}", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def truncate_table(self,request):
        try:
            table_name = request.POST.get('table_name')
            query = f"TRUNCATE TABLE {table_name}"
            mysql_conn = self.mysql_connection(request)
            cursor = mysql_conn.cursor()
            cursor.execute(query)
            mysql_conn.close()
            return JsonResponse(f"Records deleted from {table_name}", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)


