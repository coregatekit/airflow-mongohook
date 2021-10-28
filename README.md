# Airflow mongodb hook example

*** Do not forget to add mongo connection in airflow ui

Admin > Connections > Add a new record
```
Conn Id: <your connection id>
Conn Type: MongoDB
Host: <your mongodb host>
Schema: <your database>
Login: <your db user>
Password: <your db password>
Port: <your mongodb port>
```
If you have use mongodb atlas or other fill this json in `Extra` box
```
{
    "srv": true
}
```