docker stop mysqlOOD
docker rm mysqlOOD
docker run -p 3306:3306 --name=mysqlOOD -e MYSQL_ROOT_HOST=% -d mysql/mysql-server:latest 
sleep 20
PASSWORD="$(docker logs mysqlOOD 2>&1 | grep GENERATED | awk '{print $NF}')"
docker exec -it mysqlOOD mysql --connect-expired-password --user="root" --password="${PASSWORD}" -Bse "ALTER USER 'root'@'localhost' IDENTIFIED BY 'rootpassword'; CREATE USER 'usertest' IDENTIFIED by 'passwordtest';CREATE DATABASE databasetestAMySQL;CREATE DATABASE databasetestBMySQL;GRANT ALL PRIVILEGES ON databasetestAMySQL.* TO 'usertest';GRANT ALL PRIVILEGES ON databasetestBMySQL.* TO 'usertest';FLUSH PRIVILEGES;"  

