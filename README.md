# pg_statements
Simple server for "realtime" monitoring of running postgres statements (just an experiment golang + WebSockets)

psql -U kod kod < views.sql && go run main.go

Navigate browser to http://localhost:8080/
