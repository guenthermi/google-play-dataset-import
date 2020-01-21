#!/usr/bin/python3

import sys
import json
import pandas as pd
import psycopg2
import json
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from collections import Counter
from collections import defaultdict

HELP_TEXT = ('USAGE: \033[1mloader.py\033[0m dataset_base_path\n' +
             '\tdataset_base_path: path to the extracted google play store dataset folder')

# dataset url:
# https://www.kaggle.com/lava18/google-play-store-apps/

# file names without base path
APPS = 'googleplaystore.csv'
REVIEWS = 'googleplaystore_user_reviews.csv'
DB_CONFIG_PATH = 'db_config.json'

# schemes of the database tables
TABLE_SCHEMA_FILE = 'db_schema.json'


def create_connection(db_config):
    con = None
    cur = None
    # create db connection
    try:
        con = psycopg2.connect(
            "dbname='" + db_config['db_name'] + "' user='"
            + db_config['username'] + "' host='" + db_config['host']
            + "' password='" + db_config['password'] + "'")
    except:
        try:
            print("user='" + db_config['username'] + "' host='" + db_config['host']
                  + "' password='" + db_config['password'] + "'")
            con = psycopg2.connect(
                "user='" + db_config['username'] + "' host='" + db_config['host']
                + "' password='" + db_config['password'] + "'")
            print('get here')
            cur = con.cursor()
            con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(db_config['db_name'])))
            cur.close()
            con.close()
            con = psycopg2.connect(
                "dbname='" + db_config['db_name'] + "' user='"
                + db_config['username'] + "' host='" + db_config['host']
                + "' password='" + db_config['password'] + "'")
        except:
            print('ERROR: Can not connect to database')
            return
    cur = con.cursor()
    return con, cur


def is_valid_str(term):
    if type(term) == str:
        if len(term) > 0:
            return True


def parse_list(text):
    return text.split(';')


def disable_triggers(schema_info, con, cur):
    for table_name in schema_info.keys():
        cur.execute('ALTER TABLE ' + table_name + ' DISABLE trigger ALL;')
        con.commit()
    return


def enable_triggers(schema_info, con, cur):
    for table_name in schema_info.keys():
        cur.execute('ALTER TABLE ' + table_name + ' ENABLE trigger ALL;')
        con.commit()
    return


def create_schema(schema_info, con, cur):
    query_drop = "DROP TABLE IF EXISTS " + ', '.join(
        [key for key in schema_info]) + ';'
    queries_create = []
    for (name, schema) in schema_info.items():
        queries_create.append("CREATE TABLE " + name + " " + schema + ";")

    # run queries
    for query in [query_drop] + queries_create:
        cur.execute(query)
        con.commit()


def tokenize_category(cat_str):
    return ' '.join(cat_str.lower().split('_'))

# Takes the DataFrame from the movie file and extract all relevant information.


def extract_app_data(df_apps):
    # define columns which information is useful
    RELEVANT_COLUMNS = ['App', 'Category', 'Type', 'Content Rating', 'Genres']

    # reduce data frame to relevant columns
    apps_reduced = df_apps[RELEVANT_COLUMNS]

    # extract data and create output dictonary
    extracted_apps = dict()
    extracted_categories = dict()
    extracted_price_types = dict()
    extracted_content_ratings = dict()
    extracted_genres = dict()

    app_name_lookup = dict()
    category_name_lookup = dict()
    price_type_lookup = dict()
    rating_value_lookup = dict()
    genre_name_lookup = dict()

    id = 0
    for line in apps_reduced.iterrows():
        # line[0]: line number  line[1]: content
        if line[1]['App'] in app_name_lookup:
            continue
        # add simple values
        values = dict()

        if not is_valid_str(line[1]['App']):
            continue
        values['name'] = line[1]['App']

        # 1:n foreign key relations
        # TODO
        if is_valid_str(line[1]['Category']):
            category_value = tokenize_category(line[1]['Category'])
            if not category_value in category_name_lookup:
                cat_id = len(category_name_lookup)
                category_name_lookup[category_value] = cat_id
                extracted_categories[cat_id] = {
                    'id': cat_id, 'name': category_value}
            category_id = category_name_lookup[category_value]
            values['category_id'] = category_id
        if is_valid_str(line[1]['Type']):  # TODO
            price_type_value = str(line[1]['Type'])
            if not price_type_value in price_type_lookup:
                price_type_id = len(price_type_lookup)
                price_type_lookup[price_type_value] = price_type_id
                extracted_price_types[price_type_id] = {
                    'id': price_type_id, 'name': price_type_value}
            price_type_id = price_type_lookup[price_type_value]
            values['price_type'] = price_type_id
        if is_valid_str(line[1]['Content Rating']):
            rating_value = str(line[1]['Content Rating'])
            if not rating_value in rating_value_lookup:
                rating_id = len(rating_value_lookup)
                rating_value_lookup[rating_value] = rating_id
                extracted_content_ratings[rating_id] = {
                    'id': rating_id, 'rating': rating_value}
            rating_id = rating_value_lookup[rating_value]
            values['content_rating'] = rating_id

        # n:n foreign key relations
        values['genres'] = set()
        if is_valid_str(line[1]['Genres']):
            genres_list_value = parse_list(line[1]['Genres'])
            for genre_value in genres_list_value:
                if not genre_value in genre_name_lookup:
                    genre_id = len(genre_name_lookup)
                    genre_name_lookup[genre_value] = genre_id
                    extracted_genres[genre_id] = {
                        'id': genre_id, 'name': genre_value}
                genre_id = genre_name_lookup[genre_value]
                values['genres'].add(genre_id)

        extracted_apps[id] = values
        app_name_lookup[values['name']] = id
        id += 1

    return {
        'extracted_apps': extracted_apps,
        'extracted_categories': extracted_categories,
        'extracted_price_types': extracted_price_types,
        'extracted_content_ratings': extracted_content_ratings,
        'extracted_genres': extracted_genres
    }


def extract_reviews(df_reviews):

    RELEVANT_COLUMNS = ['App', 'Translated_Review']

    reviews_reduced = df_reviews[RELEVANT_COLUMNS]

    extracted_reviews = dict()

    app_review_lookup = defaultdict(set)

    id = 0
    for line in reviews_reduced.iterrows():
        values = dict()
        if not is_valid_str(line[1]['App']):
            continue
        if not is_valid_str(line[1]['Translated_Review']):
            continue

        values['app'] = str(line[1]['App'])
        values['review'] = str(line[1]['Translated_Review'])
        extracted_reviews[id] = values
        app_review_lookup[values['app']].add(id)
        id += 1
    return extracted_reviews, app_review_lookup


def process_buffers(buffers, con, cur, batch_size):
    for buffer, query in buffers.values():
        if len(buffer) >= batch_size:
            cur.executemany(query, buffer)
            con.commit()
            buffer.clear()
    return


def flush_buffers(buffers, con, cur, batch_size):
    for buffer, query in buffers.values():
        cur.executemany(query, buffer)
        con.commit()
        buffer.clear()
    return


def get_db_literal(value):
    if value == None:
        return None
    else:
        return str(value)


def insert_apps_data(data, con, cur, batch_size):
    QUERY_INSERT_APPS = (
        "INSERT INTO apps (id, name, category_id, price_type, content_rating) VALUES %s")
    QUERY_INSERT_CATEGORIES = "INSERT INTO categories (id, name) VALUES %s"
    QUERY_INSERT_TYPES = "INSERT INTO price_types (id, name) VALUES %s"
    QUERY_INSERT_RATINGS = "INSERT INTO content_ratings (id, rating) VALUES %s"
    QUERY_INSERT_GENRES = "INSERT INTO genres (id, name) VALUES %s"
    QUERY_INSERT_GENRES_RELATION = (
        "INSERT INTO apps_genres (app_id, genre_id) VALUES %s")

    apps_data = data['extracted_apps']
    buffers = {
        'apps_content': (list(), QUERY_INSERT_APPS),
        'genres_relation': (list(), QUERY_INSERT_GENRES_RELATION)
    }
    for app_id, app_values in apps_data.items():
        buffers['apps_content'][0].append([(
            get_db_literal(app_id), get_db_literal(app_values['name']),
            get_db_literal(app_values['category_id']),
            get_db_literal(app_values['price_type']),
            get_db_literal(app_values['content_rating']))])
        for genre_id in app_values['genres']:
            buffers['genres_relation'][0].append(
                [(get_db_literal(app_id), get_db_literal(genre_id))])
        process_buffers(buffers, con, cur, batch_size)
    flush_buffers(buffers, con, cur, batch_size)

    category_data = data['extracted_categories']
    buffers = {'category_content': (list(), QUERY_INSERT_CATEGORIES)}
    for category_id, category_values in category_data.items():
        buffers['category_content'][0].append(
            [(get_db_literal(category_id), get_db_literal(category_values['name']))])
        process_buffers(buffers, con, cur, batch_size)
    flush_buffers(buffers, con, cur, batch_size)

    price_type_data = data['extracted_price_types']
    buffers = {'price_type_content': (list(), QUERY_INSERT_TYPES)}
    for price_type_id, price_type_values in price_type_data.items():
        buffers['price_type_content'][0].append(
            [(get_db_literal(price_type_id), get_db_literal(price_type_values['name']))])
        process_buffers(buffers, con, cur, batch_size)
    flush_buffers(buffers, con, cur, batch_size)

    content_rating_data = data['extracted_content_ratings']
    buffers = {'rating_content': (list(), QUERY_INSERT_RATINGS)}
    for rating_id, rating_values in content_rating_data.items():
        buffers['rating_content'][0].append(
            [(get_db_literal(rating_id), get_db_literal(rating_values['rating']))])
        process_buffers(buffers, con, cur, batch_size)
    flush_buffers(buffers, con, cur, batch_size)

    genre_data = data['extracted_genres']
    buffers = {'genres_content': (list(), QUERY_INSERT_GENRES)}
    for genre_id, genre_values in genre_data.items():
        buffers['genres_content'][0].append(
            [(get_db_literal(genre_id), get_db_literal(genre_values['name']))])
        process_buffers(buffers, con, cur, batch_size)
    flush_buffers(buffers, con, cur, batch_size)

    return


def insert_review_data(data, con, cur, batch_size):
    QUERY_INSERT_REVIEWS = "INSERT INTO reviews (id, app_id, review) VALUES %s"

    buffers = {'review_content': (list(), QUERY_INSERT_REVIEWS)}
    for review_id, review_values in data.items():
        buffers['review_content'][0].append([(
            get_db_literal(review_id),
            get_db_literal(review_values['app_id']),
            get_db_literal(review_values['review']))])
        process_buffers(buffers, con, cur, batch_size)
    flush_buffers(buffers, con, cur, batch_size)

    return


def match_apps_reviews(apps_data, extracted_reviews, app_review_lookup):
    review_app_lookup = dict()  # app id -> review id
    extracted_apps = apps_data['extracted_apps']
    # match reviews to apps
    to_remove = list()
    id = 0
    for app_id, app_values in extracted_apps.items():
        if not app_values['name'] in app_review_lookup:
            to_remove.append(app_id)
            continue
        for review_id in app_review_lookup[app_values['name']]:
            extracted_reviews[review_id]['app_id'] = app_id

    # remove apps without any review
    for app_id in to_remove:
        del extracted_apps[app_id]

    # remove reviews without any app
    to_remove = list()
    for review_id, review_values in extracted_reviews.items():
        if not 'app_id' in review_values:
            to_remove.append(review_id)
    for review_id in to_remove:
        del extracted_reviews[review_id]

    return apps_data, extracted_reviews


def get_df(path):
    df = pd.read_csv(path)
    df = df.drop_duplicates()
    return df


def main(argc, argv):

    if argc != 2:
        print(HELP_TEXT)
        return

    dataset_base_path = argv[1] + '/'
    apps_path = dataset_base_path + APPS
    reviews_path = dataset_base_path + REVIEWS

    df_apps = get_df(apps_path)
    print('Read apps')
    df_reviews = get_df(reviews_path)
    print('Read reviews')

    print('Extract apps data from csv ...')
    extracted_apps = extract_app_data(df_apps)
    print('Extract review data from csv ...')
    extracted_reviews, app_review_lookup = extract_reviews(df_reviews)
    print('Match apps and reviews ...')
    extracted_apps, extracted_reviews = match_apps_reviews(
        extracted_apps, extracted_reviews, app_review_lookup)

    print('Connect to database ...')
    f_db_config = open(DB_CONFIG_PATH, 'r')
    db_config = json.load(f_db_config)
    f_db_config.close()

    con, cur = create_connection(db_config)

    batch_size = db_config['batch_size']

    # get schema
    print('Read schema file ...')
    schema_file = open(TABLE_SCHEMA_FILE, 'r')
    schema_info = json.load(schema_file)
    schema_file.close()
    print('Create Schema ...')
    create_schema(schema_info, con, cur)

    print('Insert data into database ...')
    disable_triggers(schema_info, con, cur)
    print('Insert app data ...')
    insert_apps_data(extracted_apps, con, cur, batch_size)
    print('Insert reviews ...')
    insert_review_data(extracted_reviews, con, cur, batch_size)
    enable_triggers(schema_info, con, cur)

    print('Done.')


if __name__ == "__main__":
    main(len(sys.argv), sys.argv)
