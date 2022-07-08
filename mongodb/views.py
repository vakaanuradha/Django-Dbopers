from .database import Database
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import pymongo
import csv
import json


class MongoDatabase(Database):

    def mongo_connection(self,request):
        host_name = request.POST.get('host')
        user_name = request.POST.get('username')
        password = request.POST.get('password')
        mongo_conn = pymongo.MongoClient(
                f"mongodb+srv://{user_name}:{password}@{host_name}/?retryWrites=true&w=majority"
            )
        return mongo_conn

    @csrf_exempt
    def create_schema(self, request):
        try:
            client = self.mongo_connection(request)
            schema_name = request.POST.get('schema_name')
            db = client[schema_name]
            return JsonResponse(f"Schema {schema_name} created successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def drop_schema(self, request):
        try:
            schema_name = request.POST.get('schema_name')
            client = self.mongo_connection(request)
            client.drop_database(schema_name)
            return JsonResponse(f"Schema {schema_name} dropped successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def create_table(self, request):
        try:
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            client = self.mongo_connection(request)
            db = client[schema_name]
            collection = db[table_name]
            return JsonResponse(f"Table {table_name} created successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def drop_table(self, request):
        try:
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            client = self.mongo_connection(request)
            db = client[schema_name]
            collection = db[table_name]
            collection.drop()
            return JsonResponse(f"Table {table_name} dropped successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def insert_record(self, request):
        try:
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            columns = request.POST.get('columns')
            columns = json.loads(columns)
            client = self.mongo_connection(request)
            db = client[schema_name]
            collection = db[table_name]
            collection.insert_one(columns)
            return JsonResponse(f"Data added successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def insert_multiple_records(self, request):
        try:
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            columns = request.POST.get('columns')
            columns = json.loads(columns.replace("'", '"'))
            input_file_name = request.POST.get('input_file_name')
            client = self.mongo_connection(request)
            db = client[schema_name]
            collection = db[table_name]
            with open(input_file_name) as file:
                csv_reader = csv.reader(file)
                rows_list = [{columns[i]:row[i] for i in range(len(row))}for row in csv_reader]
            collection.insert_many(rows_list)
            return JsonResponse(f"records added successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def update_records(self, request):
        try:
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            filters = request.POST.get('filters')
            filters = json.loads(filters)
            updated_values = request.POST.get('updated_values')
            updated_values = json.loads(updated_values)
            client = self.mongo_connection(request)
            db = client[schema_name]
            collection = db[table_name]
            new_record = {"$set": updated_values}
            collection.update_many(filters, new_record)
            return JsonResponse(f"records updated successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def retrieve_records(self, request):
        try:
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            client = self.mongo_connection(request)
            db = client[schema_name]
            collection = db[table_name]
            records = collection.find({}, {'_id': False})
            output = [rec for rec in records]
            return JsonResponse(output, safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def retrieve_records_with_filter(self, request):
        try:
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            filters = request.POST.get('filters')
            filters = json.loads(filters)
            filter_dict = {k: {'$in': [v]} for k, v in filters.items()}
            client = self.mongo_connection(request)
            db = client[schema_name]
            collection = db[table_name]
            records = collection.find(filter_dict, {'_id': False})
            output = [rec for rec in records]
            return JsonResponse(output, safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def delete_records_with_filter(self, request):
        try:
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            filters = request.POST.get('filters')
            filters = json.loads(filters)
            filter_dict = {k: {'$in': [v]} for k, v in filters.items()}
            client = self.mongo_connection(request)
            db = client[schema_name]
            collection = db[table_name]
            collection.delete_many(filter_dict)
            return JsonResponse("Records deleted successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def delete_records(self, request):
        try:
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            client = self.mongo_connection(request)
            db = client[schema_name]
            collection = db[table_name]
            collection.delete_many({})
            return JsonResponse("Records deleted successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def truncate_table(self, request):
        try:
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            client = self.mongo_connection(request)
            db = client[schema_name]
            collection = db[table_name]
            collection.remove()
            return JsonResponse("Records deleted successfully", safe=False)
        except Exception as er:
            return JsonResponse(f"{er}", safe=False)

    @csrf_exempt
    def health_check(self,request):
        return JsonResponse('Success1', safe=False)