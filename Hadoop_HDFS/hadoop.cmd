Environment Variable in Linux
        Only for the session once closed gone
                >> export ENV_VAL = opt/airflow/Docker
                >> echo $ENV_VAL
                >> env | grep ENV_V*
        Permanent Environment variable
                Add to ~/.bashrc 
                   >> export ENV_VAL = opt/airflow/Docker
                Reload File
                   >> source ~/.bashrc


jps
hadoop fs
hadoop fs -usage ls
hadoop fs -help ls

For data 
        git clone https://github.com/sibaramkumar/datafiles

To unzip file
        sudo apt install unzip
        unzip datafiles.zip

To list files
        ls -lrt ASC
        ls -lt DESC

To make dir
        hadoop fs -help mkdir
        hadoop fs -mkdir -p /practice/retail_db/  -p -> create if no such a file exist if already exist then this will skip the error message
                                                  -p -> Also we can use multiple subfolder like /practice/retail_db/1/2/3

To Copy the data form local to HDFS
        hadoop fs -put /datafiles/ practice/retail_db

To list the files in the HDFS
        hadoop fs -ls /practice/retail_db/datafiles/

To list recursive files in the HDFS folder
        hadoop fs -ls -R /practice/retail_db/datafiles/

To remove directory from the HDFS
        hadoop fs -help rmdir  # To get the description of the command
        hadoop fs -rmdir /practice/temp # this remove the directory only when the directory is empty
        hadoop fs -rmdir /practice/retail_db # will fail due to file is not empty
        hadoop fs -rmdir --ignore-fail-on-non-empty /practice/temp  # this will ignore fail messgase
        
        hadoop fs -help rm
        hadoop fs -rm -r /practice/retail_db/  # this will remove file -r recursive file also it will delete 
        hadoop fs -rm /practice/sample.txt
        hadoop fs -rm -f /practice/sample/txt # to avoid error message

   echo $?  This will give you previous run error output 0(success) or 1(fail)
       
        hadoop fs -rm -skipTrash /practice/sample.txt # -rm it will store in recycle bin -skipTrash this delete permantely 

From HDFS to Local
        hadoop fs -help get 
        hadoop fs -get /practice/sample.txt  .
        hadoop fs -get -f /practice/sample.txt . # To avoid error like (files exists)
        hadoop fs -get -p /practice/sample.txt . # consider in HDFS sample.txt file modified on jan-01 if you use without -p then it will copy the file to local current timestamp(sysdate)
                                                 # If we use -p then we will get exact same time stamp of the hdfs like jan-01
        hadoop fs -get /practice/retail_db/datafiles/orders/part* /practice/sample.txt . # Copy two file from different hdfs locations 

Listing and Sorting the files
        ls
        ls -l
        ls -lt # sorting based on ascending
        ls -ltr # Sorting based on descending
        hadoop fs -help ls
        hadoop fs -ls /practice/retail_db # list down using ascending by name
        hadoop fs -ls /practice/retail_db/ord* # only display the folder and subfolder or files which is start with ord
        hadoop fs -ls -r /practice/retail_db # list down using descending by name
        hadoop fs -ls -R /practice/retail_db/datafiles # this will list it down all recursive files of the folder
        hadoop fs -ls -C /practice/retail_db/datafiles # only folder/files.name will be diplayed.
        hadoop fs -ls -S /practice/retail_db/datafiles # sort by size
        hadoop fs -ls -t /practice/retail_db/datafiles # late modified files will come first

From Local to HDFS
        hadoop fs -help put
        hadoop fs -put Datafiles/datafiles/* /practice/retail_db/ # will insert only the files exists in the datafiles
        hadoop fs -put Datafiles/datafiles /practice/retail_db  # will insert datafiles folder iteself  like /datafiles/*
        hadoop fs -put -f datafiles/* /practice/retail_db/ # to avoid error
        hadoop fs -put -f -p datafiles/* /practice/retail_db/  #(overwrite the file) to match the timestamp with local

Display the Content of the File
        hadoop fs -help head
        haddop fs -head /practice/retail_db/orders/part-0000  # first 1kb of the file
        hadoop fs -tail /practice/retail_db/orders/part-0000  # bottom of the file 1kb
        hadoop fs -cat /practice/retail_db/orders/part-0000   # to display all the content of the file
        head -10 part-00000 #(unix command) this will give only first 5 lines
        hadoop fs -cat /practice/retail_db/orders/part-00000 | head -10   # hadoop command + unix command
        hadoop fs -cat /practice/retail_db/orders/part-00000 | tail -5 

Statistic of files
        hadoop fs -help stat
        hadoop fs -stat /practice/retail_db/orders/part-00000 # file last modified, by defalut %y 
        hadoop fs -stat %y /practice/retail_db/orders/part-00000 # same as first one 
        hadoop fs -stat %b /practice/retail_db/orders/part-00000 # it will give you file size in bytes
        hadoop fs -stat %F /practice/retail_db/orders/part-00000 # this is what kind og file is this (like regular file or directory)
        hadoop fs -stat %o /practice/retail_db/orders/part-00000 # this will gice block size for this file
        hadoop fs -stat %r /practice/retail_db/orders/part-00000 # this replication factor for this file.
        hadoop fs -stat %u /practice/retail_db/orders/part-00000 # creator of the file
        hadoop fs -stat %a /practice/retail_db/orders/part-00000 # permission in octal format 
        hadoop fs -stat %A /practice/retail_db/orders/part-00000 # permission in symbolic format

Storage in HDFS
        hadoop fs -help df   (for whole hdfs system)
        hadoop fs -df  # it will give all the storage of the hdfs file system.
        hadoop fs -df -h # human readble format

        hadoop fs -help du   (for individual files or folder)
        hadoop fs -du /practice/retail_db # all size storage of the individual files
        hadoop fs -du -v /practice/retail_db/ # it will header (column_name like first_name , last_name)
        hadoop fs -du -v -s /practice/retail_db # aggregated value for the retail_db folder.
        hadoop fs -du -v -s -h /practice/retail_db #  for above human readble format.
        hadoop fs -du -v -h /practice/practice # individual files human readble format.

Metadata in HDFS
        hadoop fsck -help  # this will give all the information about this folder blocks, rack, data nodes etc.
        hadoop fcsk /practice/retail_db
        use help command hadoop fsck -help 
        hadoop fsck /practice/retail_db -files # this will give files information (/practice/retail_db/categories/part-00000 1029 bytes, replicated: replication=1, 1 block(s):  OK)
        hadoop fsck /practice/retail_db -files -blocks # this more blocks level information like blocks_id etc (BP-550739962-127.0.1.1-1767296214465:blk_1073741876_1052 len=5408880 Live_repl=1)
        haddop fsck /practice/retail_db -files -blocks -locations # locations of block where it located (DatanodeInfoWithStorage[127.0.0.1:9866,DS-cc2ca55e-2cc1-411a-9c88-62cba5ef64ba,DISK])

HDFS file Permission 
        owner/user    group    others
           rwx         rwx       rwx
           r - read 
           w - write
           e - execute(Linux) | No meaning in (HDFS) All files are in HDFS is data files (no execution) 
           _rwxrwxrwx  _ -> files
           drwxrwxrwx  d -> directoory

        Change permission using -chmod
                Octal mode -chmod
                        0    ---
                        1    --x
                        2    -w-
                        3    -wx
                        4    r--
                        5    r-x
                        6    rw-
                        7    rwx
                hadoop fs -chmod 755 /practice/retail_db/orders/part-0000

                Symbolic mode 
                        u - user
                        g - group
                        o - other
                        g(group)+ r(read)  read access for group
                hadoop fs -chmod g+r /practice/retail_db/orders/part-00000

Update Properties in HDFS 
   To change the property of the file while copying the file from local to hdfs
        hdfs dfs -stat %r /practice/sample.txt # replication factor 1
        hdfs dfs -stat %o /practice/sample.txt # block size 128M

        hdfs dfs -Ddfs.blocksize = 64M -Ddfs.replication=3 -put -f /practice/sample.txt  # -f to avoid error and overwrite the file
        hdfs dfs -stat %r /practice/sample.txt
        hdfs dfs -stat %o /practice/sample.txt

        /usr/local/hadoop/etc/hadoop/   # To get hadoop configuration.

        hrithik_poojary@Hrithik:~$ vi hdfs-override-site.xml
        <configuration>
        <property>
        <name>dfs.replication</name>
        <value>5</value>
        </property>
        </configuration>

        hdfs dfs --conf hdfs-override-site.xml -put -f sample.txt /practice/sample.txt  # it will overwrite the default setting
        hdfs dfs -stat %r /practice/sample.txt

   To change change the existing file property
        hdfs fs -help setrep
        hdfs fs -setrep 3 /practice/sample.txt # to change the replication factor for the existing file








        
