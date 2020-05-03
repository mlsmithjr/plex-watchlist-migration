#
# export-to-es.py
#
# Export a Plex media database to Elasticsearch.
#
# Requires: pipenv install -or- pip install elasticsearch
#
# Just supply the URL to your Elasticsearch install followed by the path to your Plex database file
#
# Ex. python3 export-to-es.py http://192.168.2.61:9200  /volume1/Plex/Library/Application Support/Plex Media Server/Plug-in Support/Databases/com.plexapp.plugins.library.dbcom.plexapp.plugins.library.db
#
# Two indexes are added: "movies" and "tvshows".  Each time this script is run the indexes are removed and rebuilt.
#
import sys
import sqlite3
from elasticsearch import Elasticsearch


movie_sql = [
    "select  items.id, sections.name, mdata.metadata_type, mdata.title, mdata.studio, mdata.content_rating, mdata.duration,",
    "        mdata.tags_genre, mdata.tags_director, mdata.tags_writer, mdata.originally_available_at,",
    "        mdata.tags_country, mdata.tags_star, mdata.year, mdata.duration,",
    "        items.width, items.height, items.container, items.video_codec, items.audio_codec",
    "from    library_sections as sections,",
    "        metadata_items as mdata,",
    "        media_items as items",
    "where   mdata.library_section_id = sections.id",
    "and     items.metadata_item_id = mdata.id",
    "and     mdata.metadata_type = '1'"
]


series_sql = [
    "select  mdata.id, sections.name, mdata.metadata_type, mdata.title, mdata.studio, mdata.content_rating,",
    "        mdata.tags_genre, mdata.tags_director, mdata.tags_writer, mdata.originally_available_at ",
    "from    library_sections as sections,",
    "        metadata_items as mdata",
    "where   mdata.library_section_id = sections.id",
    "and     mdata.metadata_type = '2'"
]

episodes = [
    "select count()",
    "from metadata_items mi",
    "inner join metadata_items season on season.parent_id = mi.id",
    "inner join metadata_items episode on episode.parent_id = season.id",
    "where mi.metadata_type = '2'",
    "and   mi.id=?"
]


def export_movies(es: Elasticsearch, con: sqlite3.dbapi2):
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    s = ' '.join(movie_sql)
    if es.indices.exists('movies'):
        es.indices.delete('movies')
    for row in cur.execute(s).fetchall():
        rec = dict(row)
        print(rec['id'], rec['title'])
        rec['tags_director'] = rec['tags_director'].split('|')
        rec['tags_writer'] = rec['tags_writer'].split('|')
        rec['tags_star'] = rec['tags_star'].split('|')
        rec['tags_genre'] = rec['tags_genre'].split('|')
        rec['year'] = str(rec['year'])
        es.create('movies', rec['id'], rec)


def export_tv(es: Elasticsearch, con: sqlite3.dbapi2):
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    s = ' '.join(series_sql)
    if es.indices.exists('tvshows'):
        es.indices.delete('tvshows')
    for row in cur.execute(s).fetchall():
        rec = dict(row)
        mi = rec['id']
        count = cur.execute(' '.join(episodes), (mi,)).fetchone()
        rec['episodes'] = int(count[0])
        rec['tags_genre'] = rec['tags_genre'].split('|')
        es.create('tvshows', rec['id'], rec)


if __name__ == '__main__':

    es_url = sys.argv[1]
    es = Elasticsearch([es_url])
    con = sqlite3.connect(sys.argv[2])
    export_movies(es, con)
    export_tv(es, con)

