# Install ApacheSpark on Windows

- Step 1: Install [Java 8](https://java.com/en/download/).
    ```
    $ java -version
    ```
- Step 2: Install [Python 3.11 or 3.12](https://www.python.org/)
    ```
    $ python --version
    $ python -V
    ```
- Step 3: Install [Apache Spark](https://spark.apache.org/downloads.html).
    * Choose a Spark release -> 3.5.1
    * Choose a package type -> Pre-built for Apache Hadoop 3.3

    1. Create a new folder named Spark in the root of your C: drive.
    2. Move the tgz file to C:\Spark.
    2. Right-click the file and extract it to C:\Spark using the tool you have on your system (e.g., 7-Zip).
    4. Now, your C:\Spark folder has a new folder spark-3.5.1-bin-hadoop3 with the necessary files inside. 

- Step 4: Add winutils.exe File
    1. Navigate to the [winutils GitHub repository](https://github.com/cdarlint/winutils).
    2. Locate the hadoop version you need, in my case: `hadoop-3.3.6/bin`.
    3. Download the whole hadoop/bin folder where the winutils.exe file is. You can use this dowload service: [GitZip](https://kinolien.github.io/gitzip/)
    4. Create a folder named `Spark\hadoop-3.3.6\bin` in the root of your C: drive.
    5. Copy the downloaded winutils.exe file into the `C:\Spark\hadoop-3.3.6\bin` directory.

- Step 5: Configure Environment Variables
    1. Click `Start` and type `environment`. Select `Edit the system environment variables`. In the *lower-right corner*, click `Environment Variables` and then click `New` in the next window.
    2. Add:
        ```
        SPARK_HOME = C:\Spark\spark-3.5.1-bin-hadoop3
        HADOOP_HOME = C:\Spark\hadoop-3.3.6
        JAVA_HOME = C:\Program Files\Java\jdk-22
        hadoop.home.dir = C:\Spark\hadoop-3.3.6
        PYSPARK_PYTHON = C:\ProgramData\anaconda3\envs\ml\python.exe
        PYSPARK_DRIVER_PYTHON = C:\ProgramData\anaconda3\envs\ml\python.exe
        ```
    3. In the top box, click the `Path` entry from `System Variables`, then click Edit. On the right, click New.
    4. Add:
        ```
        %SPARK_HOME%\bin
        %HADOOP_HOME%\bin
        ```
        
    > You can validate the environment variables with the following commands:
    > ```
    > echo $Env:SPARK_HOME
    > echo $Env:JAVA_HOME
    > echo $Env:HADOOP_HOME
    > echo $Env:PYSPARK_PYTHON
    > echo $Env:PYSPARK_DRIVER_PYTHON
    > ```

- Step 5: Launch Spark
    1. Open a new command prompt Window using the right-click and Run as administrator:
    2. To start Spark (with Scala), enter: `spark-shell`. To close it: `:quit` or `Ctrl + d`
    3. To start Spark (with Python),
    enter: `pyspark`. To close it: `exit()` or `Ctrl + c`.
    3. The system should display several lines indicating the status of the application. Finally, the Spark logo appears, and the prompt displays the Scala shell.
    4. Open a web browser and navigate to http://localhost:4040/.
    5. To exit Spark and close the Scala shell, press ctrl-d in the command-prompt window or Exit using quit()
    6. To test Pyspark, enter the below commands in python prompt:
        ```
        from pyspark import SparkContext

        sc = SparkContext("local", "My App")

        data = ['John', 'Morrison',
                'Jacky', 'Arnold',
                'Vicky', 'Dobian']
        print(data)

        rdd = sc.parallelize(data)
        rdd.collect()
        ```

- Optional: Prepare the configuration
    1. Go to `C:\Spark\spark-3.5.1-bin-hadoop3\conf`
    2. Update `log4j2.properties` and `spark-defaults.conf`


## Documentation:
- [Install ApacheSpark on Windows](https://www.linkedin.com/pulse/install-apachespark-windows-nehal-bhagat-nbssc/)
- [Spark Connect Overview - Spark 3.5.1 Documentation](https://spark.apache.org/docs/latest/spark-connect-overview.html#download-and-start-spark-server-with-spark-connect)
    > Choose your package type, typically “Pre-built for Apache Hadoop 3.3 and later”.
- [Spark and Java versions Supportability Matrix](https://community.cloudera.com/t5/Community-Articles/Spark-and-Java-versions-Supportability-Matrix/ta-p/383669)