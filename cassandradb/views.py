from .database import Database
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv
import json
from django.shortcuts import render

class CassandraDatabase(Database):

    def connection(self, request):
        user_name = request.POST.get('username')
        password = request.POST.get('password')
        cloud_config = {
            'secure_connect_bundle': 'C:\\Users\\anuradha.manubothu\\Downloads\\secure-connect-firstdb.zip'
        }
        auth_provider = PlainTextAuthProvider(user_name, password)
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect()
        return session

    @csrf_exempt
    def create_schema(self, request):
        try:
            schema_name = request.POST.get('schema_name')
            session = self.connection(request)
            session.execute(schema_name).one()
            return JsonResponse(f"Schema {schema_name} created successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def drop_schema(self, request):
        pass

    @csrf_exempt
    def create_table(self, request):
        try:
            session = self.connection(request)
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            columns = request.POST.get('columns')
            columns = json.loads(columns)
            column_names = "(" + ", ".join(["{} {}".format(k, v) for k, v in columns.items()]) + ")"
            query = f"CREATE TABLE {schema_name}.{table_name}{column_names}"
            session.execute(query)
            return JsonResponse(f"Table {table_name} created successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def drop_table(self, request):
        try:
            session = self.connection(request)
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            query = f"DROP TABLE {schema_name}.{table_name}"
            session.execute(query)
            return JsonResponse(f"table {table_name} dropped successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def insert_record(self, request):
        try:
            session = self.connection(request)
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            columns = request.POST.get('columns')
            columns = json.loads(columns)
            column_names = "(" + ", ".join(columns.keys()) + ")"
            value_s = "(" + ", ".join(["%s" for key in columns.keys()]) + ")"
            query = f"INSERT INTO {schema_name}.{table_name} {column_names} VALUES {value_s}"
            values = tuple(columns.values())
            session.execute(query, values)
            return JsonResponse(f"Data added successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def insert_multiple_records(self, request):
        try:
            session = self.connection(request)
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            columns = request.POST.get('columns')
            columns = json.loads(columns.replace("'", '"'))
            input_file_name = request.POST.get('input_file_name')
            column_names = "(" + ", ".join(columns) + ")"
            value_s = "(" + ", ".join(["%s" for key in columns]) + ")"
            query = f"INSERT INTO {schema_name}.{table_name} {column_names} VALUES {value_s}"
            with open(input_file_name) as file:
                csv_reader = csv.reader(file)
                for row in csv_reader:
                    values = tuple(row)
                    session.execute(query, values)
            return JsonResponse(f"Data added successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def update_records(self, request):
        try:
            session = self.connection(request)
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            filters = request.POST.get('filters')
            filters = json.loads(filters)
            updated_values = request.POST.get('updated_values')
            updated_values = json.loads(updated_values)
            updated_values = ", ".join([f"{k} = '{v}'" for k, v in updated_values.items()])
            filter_values = " and ".join([f"{k} = '{v}'" for k, v in filters.items()])
            query = f"UPDATE {schema_name}.{table_name} SET {updated_values} WHERE {filter_values}"
            session.execute(query)
            return JsonResponse(f"Data updated successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def retrieve_records(self, request):
        try:
            session = self.connection(request)
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            query = f"SELECT *FROM {schema_name}.{table_name}"
            records = session.execute(query)
            output = [rec for rec in records]
            return JsonResponse(output, safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def retrieve_records_with_filter(self, request):
        try:
            session = self.connection(request)
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            filters = request.POST.get('filters')
            filters = json.loads(filters)
            filter_values = " and ".join([f"{k} = '{v}'" for k, v in filters.items()])
            query = f"SELECT *FROM {schema_name}.{table_name} WHERE {filter_values}"
            records = session.execute(query)
            output = [rec for rec in records]
            return JsonResponse(output, safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def delete_records_with_filter(self, request):
        try:
            session = self.connection(request)
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            filters = request.POST.get('filters')
            filters = json.loads(filters)
            filter_values = " and ".join([f"{k} = '{v}'" for k, v in filters.items()])
            query = f"DELETE FROM {schema_name}.{table_name} WHERE {filter_values}"
            session.execute(query)
            return JsonResponse(f"data deleted successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def delete_records(self, request):
        try:
            session = self.connection(request)
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            query = f"DELETE FROM {schema_name}.{table_name}"
            session.execute(query)
            return JsonResponse(f"data deleted successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def truncate_table(self, request):
        try:
            session = self.connection(request)
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            query = f"TRUNCATE TABLE {schema_name}.{table_name}"
            session.execute(query)
            return JsonResponse(f"data deleted successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)