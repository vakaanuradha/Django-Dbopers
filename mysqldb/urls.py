from django.urls import path,include
from .views import MysqlDatabase

mysql_db = MysqlDatabase()

urlpatterns = [
    path('health-check/', mysql_db.health_check),
    path('create_schema/', mysql_db.create_schema),
    path('drop_schema/', mysql_db.drop_schema),
    path('create_table/', mysql_db.create_table),
    path('drop_table/', mysql_db.drop_table),
    path('insert_record/', mysql_db.insert_record),
    path('insert_multiple_records/', mysql_db.insert_multiple_records),
    path('update_records/', mysql_db.update_records),
    path('retrieve_records/', mysql_db.retrieve_records),
    path('retrieve_records_with_filter/', mysql_db.retrieve_records_with_filter),
    path('delete_records_with_filter/', mysql_db.delete_records_with_filter),
    path('delete_records/', mysql_db.delete_records),
    path('truncate_table/', mysql_db.truncate_table),
]