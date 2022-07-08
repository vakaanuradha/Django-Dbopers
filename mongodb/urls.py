from django.urls import path
from .views import MongoDatabase

mongo_db = MongoDatabase()

urlpatterns = [
    path('health-check/', mongo_db.health_check),
    path('create_schema/', mongo_db.create_schema),
    path('drop_schema/', mongo_db.drop_schema),
    path('create_table/', mongo_db.create_table),
    path('drop_table/', mongo_db.drop_table),
    path('insert_record/', mongo_db.insert_record),
    path('insert_multiple_records/', mongo_db.insert_multiple_records),
    path('update_records/', mongo_db.update_records),
    path('retrieve_records/', mongo_db.retrieve_records),
    path('retrieve_records_with_filter/', mongo_db.retrieve_records_with_filter),
    path('delete_records_with_filter/', mongo_db.delete_records_with_filter),
    path('delete_records/', mongo_db.delete_records),
    path('truncate_table/', mongo_db.truncate_table),
]

