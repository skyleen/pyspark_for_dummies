import re

from pyspark import SparkConf, SparkContext

conf = (SparkConf()
        .setAppName("Map_Example")
        .setMaster("local[*]")
        )
sc = SparkContext(conf=conf)

# 1. Загрузка и предварительная обработка данных
# 1.1. Загрузите данные о постах в RDD. Предположим, что файл называется posts.csv.
posts_rdd = sc.textFile("/Users/n.shikhaleva/Downloads/posts.csv")
# 1.2. Исключите из RDD строку с заголовками (названиями столбцов)
header = posts_rdd.first()
pre_clean_rdd = (posts_rdd
                 .filter(lambda x: x != header)
                 # .map(lambda x: x.replace(",", " "))
                 )
# 1.3. Напишите функцию парсинга , которая будет преобразовывать каждую строку в кортеж Python. При этом:
# Post_id преобразуйте в int.
# comments преобразуйте в int.
# likes преобразуйте в int.
# Post_Type оставьте как строку.
def parse_posts(post):
    pattern = r'(\d+)(,{1})([A-Za-z]+)(,{1})(\d+)(,{1})(\d+)'
    m = re.match(pattern, post)
    if m:
            post_id = int(m.group(1))
            post_type = m.group(3)
            comments = int(m.group(5))
            likes = int(m.group(7))
            return (post_id, comments, likes, post_type)
    return None
clean_rdd = pre_clean_rdd.map(parse_posts)

# 2. Общая статистика вовлеченности
# 2.1. Посчитайте общее количество постов в датасете.
total_posts_count = clean_rdd.count()
print(f"Total posts count: {total_posts_count}")

# 2.2. Рассчитайте общее количество лайков по всем постам.
total_likes_count = (clean_rdd
                     .map(lambda l: l[2])
                     .reduce(lambda x, y: x + y))
print(f"Total likes count: {total_likes_count}")

# 2.3. Рассчитайте общее количество комментариев по всем постам.
total_comments_count = (clean_rdd
                     .map(lambda l: l[1])
                     .reduce(lambda x, y: x + y))
print(f"Total comments count: {total_comments_count}")

# 2.4. Определите среднее количество лайков на один пост. Округлите результат до одного знака после запятой.
avg_likes_count = round(total_likes_count / total_posts_count, 1)
print(f"Avg likes per post count: {avg_likes_count}")

# 2.5. Определите среднее количество комментариев на один пост. Округлите результат до одного знака после запятой.
avg_comments_count = round(total_comments_count / total_posts_count, 1)
print(f"Avg comments per post count: {avg_comments_count}")
print("---")
# 3. Анализ по типам постов
# 3.1. Посчитайте количество постов каждого типа.
posts_rdd = (clean_rdd
             .map(lambda x: (x[3],1))
             .reduceByKey(lambda x, y: x + y)
             .sortBy(lambda x: x[0], ascending=False))
print(f"Posts types stats:")
for p, v in posts_rdd.collect():
    print(f"{p}: {v} posts")
print("---")
# 3.2. Для каждого типа поста рассчитайте среднее количество лайков.  Округлите результат до одного знака после запятой.
likes_rdd = (clean_rdd
             .map(lambda x: (x[3],x[2]))
             .reduceByKey(lambda x, y: x + y)
             .sortBy(lambda x: x[0], ascending=False))

posts_likes_rdd = (posts_rdd
                   .join(likes_rdd)
                   .map(lambda x: (x[0],round(x[1][1]/x[1][0],1)))
                   .sortBy(lambda x: x[1], ascending=False))

print(f"Avg likes per a post stats:")
for p, v in posts_likes_rdd.collect():
    print(f"{p}: {v} likes")
print("---")

# 3.3. Для каждого типа поста рассчитайте среднее количество комментариев.  Округлите результат до одного знака после запятой.
comments_rdd = (clean_rdd
             .map(lambda x: (x[3],x[1]))
             .reduceByKey(lambda x, y: x + y)
             .sortBy(lambda x: x[0], ascending=False))

posts_comments_rdd = (posts_rdd
                   .join(comments_rdd)
                   .map(lambda x: (x[0],round(x[1][1]/x[1][0],1)))
                   .sortBy(lambda x: x[0]))

print(f"Avg comments per a post stats:")
for p, v in posts_comments_rdd.collect():
    print(f"{p}: {v} likes")
print("---")

# 4. Вовлеченность
# 4.1. Найдите Топ-5 постов с наибольшим количеством лайков. Выведите их Post_id, Post_Type , likes.
top5_posts_likes = clean_rdd.top(5, key=lambda l: l[2])
print(f"Top 5 likes per post stats:")
for p in top5_posts_likes:
     print(f"Post ID: {p[0]}, Post type: {p[3]}, Likes: {p[2]}")
print("---")

# 4.2. Найдите Топ-5 постов с наибольшим количеством комментариев. Выведите их Post_id, Post_Type, comments.
top5_posts_comments = clean_rdd.top(5, key=lambda l: l[1])
print(f"Top 5 comments per post stats:")
for p in top5_posts_comments:
     print(f"Post ID: {p[0]}, Post type: {p[3]}, Comments: {p[1]}")
print("---")

sc.stop()