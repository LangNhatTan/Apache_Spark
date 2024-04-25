# Processing Big data with Apache Spark
# Description
+ This project use Spark architecture to process big data. Our team crawl data (including job name, company name, job description, key skill) from website https://www.timesjobs.com/ and then using spark to read a big data and show it on tkinter.
# About this application
+ This project build base on Python and Spark.
+ This project use tkinter to show 1000 rows data and spark to read 100 milions data after duplicate.
+ Framework: request, beautifulSoup, pyspark, tkinter, findspark.
+ Environment: Pycharm Community Education 2022.3.3.
# Setting and run the code
+ Step 1: Setting Spark and Python (you can search how to download Spark in youtube and setting).
+ Step 2: Download folder Spark_project.
+ Step 3: Run file <b>crawl_data.py</b> to crawl data from website (Our team crawl 4500 rows in timesjobs about C# developer and write it on file csv).
+ Step 4: Run function duplicate_data in file <b>crawl_data.py</b> to duplicate data up to 100 milions or more.
+ Step 5: Run file <b>main.py</b> and show data just crawled.
# Note
+ If data is very big, you can connect many devices to decrese the time show data.
# Screenshots
+ This is a display of tkinter.

![image](https://github.com/LangNhatTan/Apache_Spark/assets/93020907/9fe03f49-4bec-49c0-a001-2576366e8525)

+ This is result
  
  ![image](https://github.com/LangNhatTan/Apache_Spark/assets/93020907/2c134b67-8697-4ad0-a457-4159b7721e2a)
