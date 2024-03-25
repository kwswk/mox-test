This is my solution for Exercise 2:

## Virtual environment creation
You are suggested to run the ETL in an virtual environment and you could start a virtual encironment with the following command:
```
cd Ex2
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Check if below packages are installed:
1. PySpark
2. kaggle


## Run Spark ETL in one command
```
python -m ex2
```

## Code explain
The code structure is simple:
1. [ex2.py](ex2.py) an entry point to run the ETL
2. [py_func](py_func) where stores ETL operation logic
   1. [py_func/kaggle_ops.py](py_func/kaggle_ops.py) Handles Kaggle API operations and Dataset preparation
   2. [py_func/spark_ops.py](py_func/spark_ops.py) Handles ALL ETL using PySpark

## My Answer to Exercise 2 / Q3
I would say it depends on business requirement that how frequent would they keep track the customer changes. The derived dataset produced in #1 stores the last screen name per day for all customers yet a customer could change multiple screen in a day. In my opion, I would start from source data as to handle potential change of requirements from business users give that computation resources is allowed.

## Sidenotes
- I didn't write any function to output spark dataframe as this is not stated in the question
- Please ignore files stored in [archived](archived)
  
## Output example

```
(.venv) erickung@EGIT230123 Ex2 % python -m ex2

Warning: Looks like you're using an outdated API Version, please consider updating (server 1.6.7 / client 1.6.6)
24/03/25 11:16:02 WARN Utils: Your hostname, EGIT230123.local resolves to a loopback address: 127.0.0.1; using 10.138.81.57 instead (on interface en0)
24/03/25 11:16:02 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/03/25 11:16:03 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
24/03/25 11:16:16 WARN GarbageCollectionMetrics: To enable non-built-in garbage collector(s) List(G1 Concurrent GC), users should configure it(them) to spark.eventLog.gcMetrics.youngGenerationGarbageCollectors or spark.eventLog.gcMetrics.oldGenerationGarbageCollectors
+----------+-------------------+----------+--------------------+--------------------+---------------+
|tweet_date|            user_id|num_tweets|            hashtags|       tweet_sources|    screen_name|
+----------+-------------------+----------+--------------------+--------------------+---------------+
|2020-03-29|1000081695545098240|         1|[#COVID19, #NHShe...|{Twitter for iPho...|      TinaBull6|
|2020-03-29|1000096695252697088|         2|[#OdishaFightsCor...|{Twitter for Andr...|dulalchandrar11|
|2020-03-29|1000096695252697088|         2|[#OdishaFightsCor...|{Twitter for Andr...|dulalchandrar11|
|2020-03-29|1000116007229419521|         1|[#Covid_19, #coro...|{Twitter for iPho...|       nattafea|
|2020-03-29|          100018348|         1|[#coronavirus, #C...|{Twitter for iPho...|  ricardo_neyra|
|2020-03-29|1000239440130924544|         1|[#Covid_19, #Coro...|{Twitter for Andr...|beingmudassir12|
|2020-03-29|1000261836636344320|         3|[#PadreNoguera, #...|{Twitter for Andr...|OmarAlcaldePSUV|
|2020-03-29|1000261836636344320|         3|[#PadreNoguera, #...|{Twitter for Andr...|OmarAlcaldePSUV|
|2020-03-29|1000261836636344320|         3|[#QuedateEnCasa, ...|{Twitter for Andr...|OmarAlcaldePSUV|
|2020-03-29|1000306795607482369|         1|          [#covid19]|{Twitter for iPho...|MontessoriWales|
|2020-03-29|1000363097138302976|         1|      [#coronavirus]|{Twitter for Andr...|       KONEKTE3|
|2020-03-29|          100047944|         2|[#StayAtHomeAndSt...|{Twitter Web App ...|    Calezcienta|
|2020-03-29|          100047944|         2|[#sabadodecuarent...|{Twitter Web App ...|    Calezcienta|
|2020-03-29|1000609675296075778|         1|         [#Covid_19]|{Twitter for Andr...|  David39007224|
|2020-03-29|1000661016135585792|         1|[#thomas, #german...|{Twitter for Andr...|AfsarPeacelover|
|2020-03-29|1000666153478320128|         1|[#Covid_19, #PPEs...|{Twitter for iPho...|      BHarbison|
|2020-03-29|1000789170074390529|         5|         [#Covid_19]|{Twitter for iPho...|    daniele_dfu|
|2020-03-29|1000789170074390529|         5|         [#Covid_19]|{Twitter for iPho...|    daniele_dfu|
|2020-03-29|1000789170074390529|         5|         [#Covid_19]|{Twitter for iPho...|    daniele_dfu|
|2020-03-29|1000789170074390529|         5|      [#coronavirus]|{Twitter for iPho...|    daniele_dfu|
+----------+-------------------+----------+--------------------+--------------------+---------------+
only showing top 20 rows

+----------+-------------------+-------------------+--------------------+------------+
|reply_date|      reply_user_id|   original_user_id|         reply_delay|tweet_number|
+----------+-------------------+-------------------+--------------------+------------+
|2020-03-29|          528479211|          528479211|INTERVAL '0 00:23...|           1|
|2020-03-29|1089269762088157184|           68297567|INTERVAL '0 00:02...|           1|
|2020-03-29|           57049566|           52544275|INTERVAL '0 06:06...|           1|
|2020-03-29|          888027091|          888027091|INTERVAL '0 00:10...|           1|
|2020-03-29|          888027091|          888027091|INTERVAL '0 00:34...|           2|
|2020-03-29|         2426459052|         2426459052|INTERVAL '0 01:32...|           1|
|2020-03-29|           15388404|           15388404|INTERVAL '0 00:01...|           1|
|2020-03-29|         4052449996|         4052449996|INTERVAL '0 00:00...|           1|
|2020-03-29|         4052449996|         4052449996|INTERVAL '0 00:13...|           2|
|2020-03-29|          367876304|          367876304|INTERVAL '0 00:11...|           1|
|2020-03-29|1026752552501686272|1026752552501686272|INTERVAL '0 00:11...|           1|
|2020-03-29|          505196984|          505196984|INTERVAL '0 00:02...|           1|
|2020-03-29| 900971346556891136| 900971346556891136|INTERVAL '0 00:11...|           1|
|2020-03-29| 900971346556891136| 900971346556891136|INTERVAL '0 00:11...|           1|
|2020-03-29|1169277580635099138|          189868631|INTERVAL '0 00:45...|           1|
|2020-03-29|           84119564|           84119564|INTERVAL '0 00:01...|           1|
|2020-03-29| 849594909031936004|1225160978737831938|INTERVAL '0 01:52...|           1|
|2020-03-29| 987660395556032512|           88940430|INTERVAL '0 00:38...|           1|
|2020-03-29|          528479211|          528479211|INTERVAL '0 00:08...|           1|
|2020-03-29| 819312344543166465| 819312344543166465|INTERVAL '0 00:01...|           1|
+----------+-------------------+-------------------+--------------------+------------+
only showing top 20 rows

+-------------------+---------------+---------------+-----------+               
|            user_id|    screen_name|old_screen_name|change_date|
+-------------------+---------------+---------------+-----------+
|1244367538529669120| BlackmanAndR88|      KanyeWI89| 2020-03-29|
| 907725168742735872|     gift_adene|    Real_Moses1| 2020-03-29|
| 907725168742735872|    Iam_Joshua5|     gift_adene| 2020-03-29|
|1092355973975552000|Engr_Azhar_Shah|        EazharS| 2020-03-29|
|1110883404491444225|PurpleIsPowrful|ruthforcongress| 2020-03-29|
|1189542607245852673|      mskltccrc|HI66iiMDHyzU8oM| 2020-03-29|
|1192900241613377539| ImForAllHumans|   Michael4Yang| 2020-03-29|
|1224792058210267136|  AmiRFaRooq787|  AmiRFaRooq789| 2020-03-29|
|1228380016519647235|IprQa_AcPqIlIzx|ipr_ioZvQaPlIIb| 2020-03-29|
|1229771933518045184|AlbaBarrenetxea|   Alba02208797| 2020-03-29|
|1238870792500391936|     StoicUnity|   Tu_mamA_mama| 2020-03-29|
|1243516299067772928|  Infosglobales| InfosDigitales| 2020-03-29|
|          449093135|     itsriavale|     valelaltra| 2020-03-29|
|          461841349|        ZeeNews|   ZeeNewsHindi| 2020-03-29|
|         4835167894|       seljak_b|        SeljakB| 2020-03-29|
| 964249757223878656| wakabalapakaba| wakabalaplante| 2020-03-29|
| 964249757223878656| wakabalaplante| wakabalapakaba| 2020-03-29|
| 964249757223878656| wakabalapakaba| wakabalaplante| 2020-03-29|
|          330014113|        YrBases| 90SecondCrafts| 2020-03-29|
|          399536708|      SaraZzeta|   SaraZarazaga| 2020-03-29|
+-------------------+---------------+---------------+-----------+
only showing top 20 rows
```

