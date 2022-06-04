from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import expr, col, count, split, explode, udf, sum, date_format, to_date, broadcast
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType, ArrayType

from myFunctions import extract_languages, extract_domains

if __name__ == '__main__':
    # 2: Đọc dữ liệu từ MongoDB với Spark
    # CÓ thể cần thêm .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    # Nếu trong tệp D:\spark-3.1.2-bin-hadoop3.2\conf\spark-defaults.conf đã có cấu hình spark.jars.packages
    # với giá trị org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 thì không cần thêm lúc tạo Session.
    spark = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('MyApp') \
        .config('spark.mongodb.input.uri', 'mongodb://localhost/dep303_asm1.questions') \
        .enableHiveSupport() \
        .getOrCreate()

    # 3. Đọc từ questions DB và Chuẩn hóa kiểu dữ liệu
    df1 = spark.read \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .load()
    questions_df = df1.withColumn('OwnerUserId', col('OwnerUserId').cast(IntegerType())) \
        .withColumn('CreationDate', col('CreationDate').cast(DateType())) \
        .withColumn('ClosedDate', expr("case when ClosedDate == 'NA' then null else cast(CreationDate as date) end"))
    print('QUESTIONS SCHEMA')
    questions_df.printSchema()

    # 4. Yêu cầu 1: Tính số lần xuất hiện của các ngôn ngữ lập trình
    """
    print('NUMBER 4')
    extract_languages_udf = udf(extract_languages, returnType=ArrayType(StringType()))
    # chi co the truyen tham so la cac columns trong UDF
    languages_df = questions_df.withColumn('Body_extract', extract_languages_udf(col('Body'))) \
        .withColumn('Programing Language', explode('Body_extract')) \
        .groupBy('Programing Language') \
        .agg(count('Programing Language').alias('Count'))
    languages_df.show()
    """
    # 5. Yêu cầu 2 : Tìm 20 domains được sử dụng nhiều nhất trong các câu hỏi

    print('NUMBER 5')
    questions_df = questions_df.repartition(10)
    extract_domains_udf = udf(extract_domains, returnType=ArrayType(StringType()))
    domains_df = questions_df.withColumn('Body_extract', extract_domains_udf(col('Body'))) \
        .withColumn('Domain', explode(col('Body_extract'))) \
        .groupBy('Domain') \
        .agg(count('Domain').alias('Count')) \
        .sort(col('Count').desc())
    domains_df.show()

    # 6. Yêu cầu 3 : Tính tổng điểm của User theo từng ngày

    # print('NUMBER 6')
    # # questions_df = questions_df.repartition(4)
    # running_total_window = Window.partitionBy('OwnerUserId') \
    #     .orderBy('OwnerUserId', 'CreationDate') \
    #     .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    # questions_df.dropna(subset=['OwnerUserId']) \
    #     .select('OwnerUserId', 'CreationDate', 'Score') \
    #     .orderBy('OwnerUserId', 'CreationDate').show()
    # scorePerDay_df = questions_df.withColumn("TotalScore", sum('Score').over(running_total_window))
    # scorePerDay_df.dropna(subset=['OwnerUserId']) \
    #     .select('OwnerUserId', 'CreationDate', 'TotalScore') \
    #     .orderBy('OwnerUserId', 'CreationDate').show()

    # 7 Yêu cầu 4: Tính tổng số điểm mà User đạt được trong một khoảng thời gian
    """
    print('NUMBER 7')
    START = "2010-05-27"
    END = '2011-11-27'

    questions_df.dropna(subset=['OwnerUserId']) \
        .select('OwnerUserId', 'CreationDate', 'Score') \
        .orderBy('OwnerUserId', 'CreationDate') \
        .show()
    scorePerRange_df = questions_df.filter((START < col('CreationDate')) & (col('CreationDate') < END)) \
        .dropna(subset=['OwnerUserId']) \
        .groupBy('OwnerUserId') \
        .agg(sum('Score').alias('TotalScore')) \
        .orderBy('OwnerUserId')
    scorePerRange_df.show()
    """
    # 3. Đọc từ answers DB và Chuẩn hóa kiểu dữ liệu
    """
    df2 = spark.read \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .option("uri", "mongodb://127.0.0.1/dep303_asm1.answers") \
        .load()
    answers_df = df2.withColumn('OwnerUserId', col('OwnerUserId').cast(IntegerType())) \
        .withColumn('CreationDate', col('CreationDate').cast(DateType()))
    print('ANSWERS SCHEMA')
    answers_df.printSchema()
    """
    # 8. Yêu cầu 5: Tìm các câu hỏi có nhiều câu trả lời

    print('NUMBER 8')
    '''
    # Code for bucketing data. Run only once
    # Xoa 2 folders: meta, spark-warehouse + file derby.log trước khi chạy lại
    spark.sql("Create database if not exists Bucketed_DB")
    spark.sql("USE Bucketed_DB")
    questions_df.coalesce(1).write \
        .bucketBy(20, 'Id') \
        .mode('overwrite') \
        .saveAsTable('Bucketed_DB.myQuestions')

    answers_df.coalesce(1).write \
        .bucketBy(20, 'ParentId') \
        .mode('overwrite') \
        .saveAsTable('Bucketed_DB.myAnswers')
    '''
    # df3 = spark.read.table('Bucketed_DB.myQuestions')
    # df4 = spark.read.table('Bucketed_DB.myAnswers')
    # # Tránh thao tác shuffle khi join
    # spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    # spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
    # # Điều kiện join là ID câu hỏi
    # join_expr = df3.Id == df4.ParentId
    # # Đổi ID của df4 trước, tránh xung đột khi chọn cả 2 cột ID sau khi join
    # df4.withColumnRenamed('Id', 'Answer ID') \
    #     .join(df3, join_expr, 'inner') \
    #     .select(col('Id').alias('Question ID'), 'Answer ID') \
    #     .groupBy('Question ID') \
    #     .agg(count('Answer ID').alias('Total Answers')) \
    #     .orderBy(col('Question ID').asc()) \
    #     .filter(col('Total Answers') > 5) \
    #     .show()

    # 9. (Nâng cao) Yêu cầu 6: Tìm các Active User
    """
    print('NUMBER 9')
    '''
    # Code for bucketing data. Run only once
    spark.sql("Create database if not exists Bucketed_DB")
    spark.sql("USE Bucketed_DB")
    questions_df.coalesce(1).write \
        .bucketBy(20, 'Id') \
        .mode('overwrite') \
        .saveAsTable('Bucketed_DB.myQuestions')

    answers_df.coalesce(1).write \
        .bucketBy(20, 'ParentId') \
        .mode('overwrite') \
        .saveAsTable('Bucketed_DB.myAnswers')
    '''
    df3 = spark.read.table('Bucketed_DB.myQuestions')
    df4 = spark.read.table('Bucketed_DB.myAnswers')
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
    # Điều kiện join là ID câu hỏi
    join_expr = df3.Id == df4.ParentId
    # JOIN và sửa tên một số cột, tránh trùng lặp
    join_df = df4.withColumnRenamed('Id', 'Answer ID') \
        .withColumnRenamed('OwnerUserId', 'Responder_ID') \
        .withColumnRenamed('CreationDate', 'Answer CreationDate') \
        .withColumnRenamed('Score', 'Answer Score') \
        .join(df3, join_expr, 'inner') \
        .drop('ParentId') \
        .withColumnRenamed('Id', 'Question ID') \
        .withColumnRenamed('OwnerUserId', 'Questioner ID') \
        .withColumnRenamed('CreationDate', 'Question CreationDate') \
        .withColumnRenamed('Score', 'Question Score')
    # join_df.show()
    
    # Điều kiện thứ 1: User Có nhiều hơn 50 câu trả lời hoặc tổng số điểm đạt được khi trả lời lớn hơn 500.
    # Group By ID người trả lời -> đếm số câu trả lời và tổng điểm -> filter
    request_1_df = join_df.groupBy('Responder_ID') \
        .agg(count('Answer ID').alias('Total Answers'),
             sum('Answer Score').alias('Total Answer Score')) \
        .filter((col('Total Answers') > 50) | (col('Total Answer Score') > 500))
    # request_1_df.show()

    # Điều kiện thứ 2: User Có nhiều hơn 5 câu trả lời ngay trong ngày câu hỏi được tạo. Filter
    # các câu trả lời ngay trong ngày câu hỏi được đăng -> Group By ID người trả lời -> đếm số câu trả lời -> filter
    request_2_df = join_df.filter(col('Question CreationDate') == col('Answer CreationDate')) \
        .groupBy('Responder_ID') \
        .agg(count('Answer ID').alias('Total Answers')) \
        .filter(col('Total Answers') > 5)
    # request_2_df.show()

    # JOIN để được các Active Users
    # request_1_df có khoảng 4k dòng, request_2_df có khoảng 50k dòng, nên em broadcast request_1_df vào request_2_df
    request_2_df.join(broadcast(request_1_df), request_1_df.Responder_ID == request_2_df.Responder_ID, 'inner') \
        .drop(request_2_df.Responder_ID) \
        .select(col('Responder_ID').alias('Active_User_ID')) \
        .orderBy('Active_User_ID') \
        .show()
    """
    input('Wait a key...')
