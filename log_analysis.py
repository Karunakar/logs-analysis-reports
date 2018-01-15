#! /usr/bin/env python

import psycopg2

DBNAME = "news"

def connect():
    return psycopg2.connect("dbname=news")


def pull_query_results_from_db(query):
    db = connect()
    c = db.cursor()
    c.execute(query)
    results = c.fetchall()
    return results

	
def display_query_results(query_results):
    for i in range(len(query_results)):
        title = query_results[i][1]
        author_views = query_results[i][0]
        print("    %s -- %d" % (title, author_views))


def display_error_results(query_results):
    for i in range(len(query_results)):
        print(
            "    %s -- %s   %s "%
            (query_results[i][0], query_results[i][1], "Errors"))

print("*******************************************************")
print("1. What are the most popular three articles of all time?")

query_for_first_3_records = "SELECT count(title) \
    AS author_views, title FROM articles, log \
    WHERE log.path LIKE concat('%',articles.slug) \
    GROUP BY articles.title, articles.author ORDER BY author_views DESC  limit 3"


get_first_3_articles = pull_query_results_from_db(query_for_first_3_records)

display_query_results(get_first_3_articles)

print("*******************************************************")
print("\n")


print("2. Who are the most popular article authors of all time?")

query_for_most_popular_authors = "select  count(*) as author_views , authors.name from articles inner \
join authors on articles.author = authors.id inner join log \
    on log.path like concat('%', articles.slug, '%') \
    group by authors.name order by author_views desc"
get_famous_authors = pull_query_results_from_db(query_for_most_popular_authors)
get_famous_authors = display_query_results(get_famous_authors)

print("*******************************************************")
print("\n")


print("3. On which days did more than 1% of requests lead to errors?")
query_to_get_date_errors_morethan_1 = "select error_date, percentage from ( \
    select error_date, round((sum(requests)/(select count(*) from log where \
    substring(cast(log.time as text), 0, 11) = error_date) * 100), 2) as \
    percentage from (select substring(cast(log.time as text), 0, 11) as error_date, \
    count(*) as requests from log where status like '%4%' group by error_date) \
    as log_percentage group by error_date order by percentage desc) as last_query \
    where percentage >= 1  "

get_famous_authors = pull_query_results_from_db(query_to_get_date_errors_morethan_1)
get_famous_authors = display_error_results(get_famous_authors)
print("*******************************************************")
