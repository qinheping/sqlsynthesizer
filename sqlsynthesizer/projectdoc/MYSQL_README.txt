1. Install MySQL version 5.5 (other version might work too)

2. Configure MySQL's database using the file: ./dat/mysql.config

   (an optional step is to inspect the database:
    lanunch the command line: type >>mysql.exe
    then: connect sylsynth
    then: any sql query you want)
    
    (shutdown the database service)

3. Launch MySQL service in the background:  
   in Windows: >> mysqld.exe --console
   or
   >>mysqld.exe -u root
   
   More info: http://dev.mysql.com/doc/refman/5.0/en/windows-start-command-line.html
   