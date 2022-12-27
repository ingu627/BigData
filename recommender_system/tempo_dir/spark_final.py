from __future__ import print_function

if __name__ == '__main__':

    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
    from pyspark.ml.recommendation import ALS
    from pyspark.ml.evaluation import RegressionEvaluator
    import pandas as pd
    
    MAX_MEMORY = '8g'

    spark = SparkSession.builder \
        .appName('recommender_system1') \
        .config('spark.driver.memory', MAX_MEMORY) \
        .config('spark.some.config.option', 'some-value') \
        .getOrCreate()

    base_path = '/Users/hyunseokjung/data/movie_dataset/'

    print('\n\nLoad Movie Dataset : ratings, movies, links\n\n')
    ratings = spark.read.csv(base_path+'ratings.csv', header=True, inferSchema=True).repartition(5).cache()
    metadata = spark.read.csv(base_path+'movies_metadata.csv', header=True, inferSchema=True).repartition(5).cache()
    links = spark.read.csv(base_path+'links.csv', header=True, inferSchema=True).repartition(5).cache()

    ratings = ratings.select('userId', 'movieId', 'rating').cache()
    print('Transform : ratings\n')
    print(ratings.show(3))
    print(f'UserId Count : {ratings.count()}')

    metadata = metadata.select('imdb_id', 'title', 'vote_average', 'release_date').cache()
    print('Transform : movies\n')
    print(metadata.show(3))
    print(f'Movie Count : {metadata.count()}')

    train, test = ratings.randomSplit([.7, .3], seed=42)

    print('\nRecommender-Model-ALS\n\n')

    als = ALS(
        rank=30,
        maxIter=4,
        regParam=0.1,
        userCol='userId',
        itemCol='movieId',
        ratingCol='rating',
        coldStartStrategy='drop',
        implicitPrefs=False
    )
    model = als.fit(train)
    predictions = model.transform(test)

    evaluator = RegressionEvaluator(metricName='mae', labelCol='rating',
                                    predictionCol='prediction')

    mae = evaluator.evaluate(predictions)
    print(f'MAE (Test) = {mae}\n\n')
    
    user_id = int(input('INPUT USER_ID : '))
    
    user_suggest = test.filter(test['userId'] == user_id).select(['movieId', 'userId'])
    user_offer = model.transform(user_suggest)
    user_offer.orderBy('prediction', ascending=False).show(7)
    
    
    metadata_pd = metadata.toPandas()
    links_pd = links.toPandas()
    
    def get_movie_metadata(movieId):
        metadata_pd['imdb_id'] = metadata_pd['imdb_id'].astype('category')
        imdb_id = links_pd[links_pd['movieId'] == movieId]
        imdb_id = imdb_id.imdbId.values[0]
        if len(str(imdb_id)) == 7:
            movie_rated = metadata_pd[metadata_pd['imdb_id'] == 'tt'+imdb_id.astype(str)]
            df = movie_rated.loc[:,['title', 'vote_average', 'release_date']]
            return df.reset_index(drop=True)
        elif len(str(imdb_id)) == 6:
            movie_rated = metadata_pd[metadata_pd['imdb_id'] == 'tt0'+imdb_id.astype(str)]
            df = movie_rated.loc[:,['title', 'vote_average', 'release_date']]
            return df.reset_index(drop=True)
        elif len(str(imdb_id)) == 5:
            movie_rated = metadata_pd[metadata_pd['imdb_id'] == 'tt00'+imdb_id.astype(str)]
            df = movie_rated.loc[:,['title', 'vote_average', 'release_date']]
            return df.reset_index(drop=True)
        elif len(str(imdb_id)) == 4:
            movie_rated = metadata_pd[metadata_pd['imdb_id'] == 'tt000'+imdb_id.astype(str)]
            df = movie_rated.loc[:,['title', 'vote_average', 'release_date']]
            return df.reset_index(drop=True)
        elif len(str(imdb_id)) == 3:
            movie_rated = metadata_pd[metadata_pd['imdb_id'] == 'tt0000'+imdb_id.astype(str)]
            df = movie_rated.loc[:,['title', 'vote_average', 'release_date']]
            return df.reset_index(drop=True)
        elif len(str(imdb_id)) == 2:
            movie_rated = metadata_pd[metadata_pd['imdb_id'] == 'tt00000'+imdb_id.astype(str)]
            df = movie_rated.loc[:,['title', 'vote_average', 'release_date']]
            return df.reset_index(drop=True)
        elif len(str(imdb_id)) == 1:
            movie_rated = metadata_pd[metadata_pd['imdb_id'] == 'tt000000'+imdb_id.astype(str)]
            df = movie_rated.loc[:,['title', 'vote_average', 'release_date']]
            return df.reset_index(drop=True)
        else:
            pass
    
    df_movie = pd.DataFrame({'title': ['aaa'], 
                         'vote_average': [1.7], 
                         'release_date': ['1999-01-01']
        })
    
    user_offer_order = user_offer.orderBy('prediction', ascending=False).toPandas()
    
    for movieId in user_offer_order['movieId']:   
        df_movie = pd.concat([df_movie, get_movie_metadata(movieId)])
    
    print(df_movie.head(7))

# Movie-dataset Setting

# Zookeeper Server 
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/zookeeper-server-start.sh /Users/hyunseokjung/kafka_2.12-3.3.1/config/zookeeper.properties
# Kafka Server 
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-server-start.sh /Users/hyunseokjung/kafka_2.12-3.3.1/config/server.properties

# kafka connect
# /Users/hyunseokjung/confluent-7.3.0/bin/connect-distributed -daemon /Users/hyunseokjung/confluent-7.3.0/etc/kafka/connect-distributed.properties

# Kafka Topic
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --topic userid --partitions 1 --replication-factor 1
# Kafka Producer
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic userid
# Kafka Producer - create file
# > echo "plugin.path=/Users/hyunseokjung/kafka_2.12-3.3.1/libs/connect-file-3.3.1.jar"
# > echo -e "50\n30\n200" > userId.txt
# Kafka Consumer
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic userid


# kafka topic list
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list
# kafka topic delete
# /Users/hyunseokjung/kafka_2.12-3.3.1/bin/kafka-topics.sh --delete --bootstrap-server 127.0.0.1:9092 --topic userid