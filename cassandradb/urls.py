from django.urls import path
from .views import CassandraDatabase

cassandra_db = CassandraDatabase()

urlpatterns = [

    path('create_schema/', cassandra_db.create_schema),
    path('drop_schema/', cassandra_db.drop_schema),
    path('create_table/', cassandra_db.create_table),
    path('drop_table/', cassandra_db.drop_table),
    path('insert_record/', cassandra_db.insert_record),
    path('insert_multiple_records/', cassandra_db.insert_multiple_records),
    path('update_records/', cassandra_db.update_records),
    path('retrieve_records/', cassandra_db.retrieve_records),
    path('retrieve_records_with_filter/', cassandra_db.retrieve_records_with_filter),
    path('delete_records_with_filter/', cassandra_db.delete_records_with_filter),
    path('delete_records/', cassandra_db.delete_records),
    path('truncate_table/', cassandra_db.truncate_table),
]