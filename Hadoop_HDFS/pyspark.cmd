>> pyspark # this will create sparksession (spark) and sparkcontext (sc) automatically

>> sc 

>> spark 

>> spark.sparkContext 

To run python function 2 ways 
   activate env 
        >> python sparksession.py
   spark-submit 
        >> spark-submit sparksession.py
        >> spark-submit -master (client(local) or cluster(yarn)) --deploy-mode (client (logs becuase local run) or cluster(no logs becuase)) sparksession.py
   To get the logs when we run in cluster mode 
        When you run Spark on YARN in cluster deploy mode, the driver runs inside a YARN container, not in your terminal. Therefore, 
        all application logs must be fetched from YARN, not from stdout.
        >> yarn application -list -appStates ALL 
        >> yarn logs -applicationId <application_id>
        >> yarn logs -applicationId <application_id> | tail -n 50
        >> yarn logs -applicationId <application_id> -log_files stdout # only driver output
        >> yarn logs -applicationId <application_id> -log_files stderr # only error
        grep -i  # case insensitive
             -n  # show linenumbers + result
             -v  # invert match -Remove noise  
        >> yarn logs -applicationId application_1767443207152_0001 | grep -i spark  # to find the keywords
        >> yarn logs -applicationId application_1767443207152_0001 | grep -n spark  # line + result
        >> yarn logs -applicationId application_1767443207152_0001 -log_files stderr | grep -i exception | tail -n 20

  Shuffle Partitons
       >> spark-submit --conf spark.sql.shuffle.partitions=300 sparksession02Partitions.py # by default 200
                # spark.sql.shuffle.partitions controls how many output partitions Spark creates after a shuffle 
                  operation in Spark SQL / DataFrame workloads.
                # During a shuffle:
                       1) Data is repartitioned by key
                       2) Each partition becomes a task
                       3) Tasks run in parallel on executors
                # After a shuffle, Spark will create 300 partitions, resulting in 300 tasks for the next stage.
                # Problems if value is too low
                       1) Large partitions
                       2) Long-running tasks
 Yarn Environment
        >> spark-submit --conf spark.yarn.appMasterEnv.HDFS_PATH="opt/usr/Docker/Airflow" sparksession03YarnEnv.py
                # spark.yarn.appMasterEnv.HDFS_PATH="opt/usr/Docker/Airflow" is used to inject an environment variable 
                  into the Spark ApplicationMaster (AM) process when running on YARN.
                # spark.yarn.appMasterEnv.*
                  → Prefix used to set environment variables for the YARN ApplicationMaster
                # HDFS_PATH
                  → Name of the environment variable
                # "opt/usr/Docker/Airflow"
                  → Value assigned to that variable
                
                # When Spark runs in YARN cluster mode:
                        Your driver runs inside the ApplicationMaster container
                        The driver does not inherit your local shell environment
                        Any environment variable used in your Spark code must be explicitly passed
                # This option ensures that HDFS_PATH is available to:
                        Spark driver (ApplicationMaster)
                        Any logic executed before executors start
