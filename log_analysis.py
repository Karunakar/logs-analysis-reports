#! /usr/bin/env python

import psycopg2

DBNAME = "news"


def connect():
    return psycopg2.connect("dbname=news")


def get_query_results(query):
    db = connect()
    c = db.cursor()
    c.execute(query)
    results = c.fetchall()
    return results


def display_query_results(query_results):
    for i in range(len(query_results)):
        title = query_results[i][0]
        views = query_results[i][1]
        print("%s == %d" % (title, views))


def display_error_results(query_results):
    
    for i in range(len(query_results)):
        print(
            "%s == %s  == %s" %
            (query_results[i][0], query_results[i][1]), "Errors"	)


print("What are the most popular three articles of all time?")

query_for_first_3_records = "SELECT title, count(title) \
    AS views FROM articles, log \
    WHERE log.path LIKE concat('%',articles.slug) \
    GROUP BY articles.title, articles.author ORDER BY views DESC "


get_first_3_articles = get_query_results(query_for_first_3_records)

display_query_results(get_first_3_articles)


print("What are the most popular three articles of all time?")


query_for_most_popular_authors = "select authors.name, count(*) as views from articles inner \
join authors on articles.author = authors.id inner join log \
    on log.path like concat('%', articles.slug, '%') \
    group by authors.name order by views desc"


get_famous_authors = get_query_results(query_for_most_popular_authors)

print("On which days did more than 1% of requests lead to errors?")
query_to_get_date_errors_morethan_1 = "select day, perc from ( \
    select day, round((sum(requests)/(select count(*) from log where \
    substring(cast(log.time as text), 0, 11) = day) * 100), 2) as \
    perc from (select substring(cast(log.time as text), 0, 11) as day, \
    count(*) as requests from log where status like '%404%' group by day) \
    as log_percentage group by day order by perc desc) as final_query \
    where perc >= 1  "
	
get_famous_authors = get_query_results(query_to_get_date_errors_morethan_1)
	
get_famous_authors = display_error_results(get_famous_authors)

