import sqlite3
import os
import pathlib
from typing import Dict

plex_db_path = 'com.plexapp.plugins.library.db'
con = sqlite3.connect(plex_db_path)
con.row_factory = sqlite3.Row
cur = con.cursor()

backup_db_path = 'backup.db'
bcon = sqlite3.connect(backup_db_path)
bcon.row_factory = sqlite3.Row
bcur = bcon.cursor()

log_db_path = 'log.db'
lcon = sqlite3.connect(log_db_path)
lcon.row_factory = sqlite3.Row
lcur = lcon.cursor()

section_name = 'Mark'
plex_metadata_base = '/mnt/b/plex/Metadata/'
media_base_subst = ('/Volumes/merger/media/video', '/mnt/b/media/video')

newlocations = dict()
newdirectories = dict()
newsections = dict()
new_metadata_items = dict()
new_media_items = dict()
new_media_parts = dict()
newtags = dict()


def subst_name(name: str):
    return name.replace(*media_base_subst)


def save_map(table: str, m: dict):
    #for k, v in m.items():
    #    lcur.execute('insert into map values (?,?,?)', (table, k, v))
    pass


def insert_map(table: str, oldid, newid):
    #lcur.execute('insert into map values (?,?,?)', (table, oldid, newid))
    pass


def new_row_id(table: str, oldid: str):
#    result = lcur.execute('select newid from map where name=? and oldid=?', (table, oldid))
#    row = result.fetchone()
#    if row:
#        return row[0]
    return None


def do_insert(dest_name: str, drow: Dict, oldid=None):
    if oldid:
        new_id = new_row_id(dest_name, oldid)
        if new_id:
            return new_id

    clist = list()
    vals = list()
    for c in list(drow.keys()):
        if c == 'id':
            continue
        if drow[c] is None:
            continue
        clist.append(c)
        vals.append(drow[c])

    plist = ','.join(['?'] * len(clist))
    clist_str = ','.join(['"' + c + '"' for c in clist])
    s = f"insert into {dest_name} ({clist_str}) values ({plist})"
    assert (len(vals) == len(clist))
    new_id = cur.execute(s, vals).lastrowid
    insert_map(dest_name, oldid, new_id)
    return new_id


# def get_map(table: str):
#     equiv = dict()
#     for r in lcur.execute('select * from map where name=?', (table,)):
#         row = dict(r)
#         equiv[row['oldid']] = row['newid']
#     return equiv


# def copy_rows(table_name, sql):
#     equiv = dict()
#     result = bcur.execute(sql)
#     for r in result.fetchall():
#         row = dict(r)
#         oldid = row['id']
#         rowid = do_insert(table_name, r, oldid)
#         equiv[oldid] = rowid
#     save_map(table_name, equiv)
#     # get full map, bringing in already exist pairs too
#     return get_map(table_name)


def fix(d: Dict, key: str, amap: Dict):
    d[key] = amap[d[key]]


def rebind(table_name: str, sql: str, params=None, preprocessor=None, postprocessor=None):
    if params:
        result = bcur.execute(sql, params)
    else:
        result = bcur.execute(sql)
    newmap = dict()
    for row in result.fetchall():
        drow = dict(row)
        old_id = drow['id']
        if preprocessor:
            if not preprocessor(drow):
                continue
        new_id = do_insert(table_name, drow, drow['id'])
        if postprocessor:
            postprocessor(row, new_id, old_id)
        newmap[old_id] = new_id
    return newmap


def section_locations_pre(row: Dict):
    if media_base_subst:
        row['root_path'] = row['root_path'].replace(*media_base_subst)
    return True


def directories_pre(row: Dict):
    global newsections, newdirectories

    parent_id = row['parent_directory_id']
    fix(row, 'library_section_id', newsections)
    if parent_id:
        fix(row, 'parent_directory_id', newdirectories)
    return True


def directories_post(row: Dict, new_id, old_id):
    global newdirectories

    newdirectories[old_id] = new_id


def metadata_item_pre(row: Dict):
    global newsections

    # JUST DOING MOVIES RIGHT NOW
    if row['metadata_type'] != 1:
        return False

    fix(row, 'library_section_id', newsections)
    parent_id = row['parent_id']
    if parent_id:
        print(f"unexpected parent_id {parent_id} for {row['name']}")
        return False
    return True


def media_item_pre(row: Dict):
    global newsections, new_metadata_items, newlocations

    fix(row, 'library_section_id', newsections)
    fix(row, 'metadata_item_id', new_metadata_items)
    fix(row, 'section_location_id', newlocations)
    return True


def media_stream_pre(row: Dict):
    global new_media_items, new_media_parts

    fix(row, 'media_item_id', new_media_items)
    fix(row, 'media_part_id', new_media_parts)
    return True


def media_item_post(row: Dict, new_id, old_id):
    global new_media_items
    new_media_items[old_id] = new_id
    rebind('media_parts', 'select * from media_parts where media_item_id=?', (old_id,), media_parts_pre, media_parts_post)
    rebind('media_streams', 'select * from media_streams where media_item_id=?', (old_id,), media_stream_pre)


def media_parts_pre(row: Dict):
    global newdirectories

    fix(row, 'media_item_id', new_media_items)
    fix(row, 'directory_id', newdirectories)
    if media_base_subst:
        row['file'] = row['file'].replace(*media_base_subst)
    return True


def media_parts_post(row: Dict, new_id, old_id):
    global new_media_parts

    new_media_parts[old_id] = new_id


def metadata_item_post(row: Dict, new_id, old_id):
    global new_metadata_items

    new_metadata_items[old_id] = new_id
    rebind('media_items', 'select * from media_items where metadata_item_id=?', (old_id,), media_item_pre, media_item_post)
    rebind('taggings', 'select * from taggings where metadata_item_id=?', (old_id,), taggings_pre)



def taggings_pre(row:Dict):
    global newtags
    fix(row, 'tag_id', newtags)
    return True


def restore_movies():
    global newsections, newlocations, newdirectories, newtags

    lcur.execute('create table if not exists map (name text, oldid number, newid number)')

    newsections = rebind('library_sections', 'select * from library_sections where section_type in (1,2)')
    newlocations = rebind('section_locations', 'select * from section_locations', None, section_locations_pre)
    newdirectories = rebind('directories', 'select * from directories', None, directories_pre, directories_post)
    newtags = rebind('tags', 'select * from tags')

    rebind('metadata_items', 'select * from metadata_items where library_section_id is not null', None, metadata_item_pre, metadata_item_post)
    con.commit()
    lcon.commit()
    exit(0)

    #
    # get the items
    #
    new_mdata_items = dict()
    for md_item in bcur.execute('select * from metadata_items where library_section_id is not null').fetchall():
        mdata_item: dict = dict(md_item)
        oldid = mdata_item['id']

        # JUST DOING MOVIES RIGHT NOW
        if mdata_item['metadata_type'] != 1:
            continue

        fix(mdata_item, 'library_section_id', newsections)
        parent_id = mdata_item['parent_id']
        if parent_id:
            print(f"unexpected parent_id {parent_id} for {mdata_item['name']}")
            exit(1)

        newid = do_insert('metadata_items', md_item, oldid)
        new_mdata_items[oldid] = newid

        media_item = dict(bcur.execute('select * from media_items where metadata_item_id=?', (oldid,)).fetchone())
        fix(media_item, 'library_section_id', newsections)
        media_item['metadata_item_id'] = newid
        fix(media_item, 'section_location_id', newlocations)
        media_item_id = media_item['id']
        new_media_item_id = do_insert('media_items', media_item, media_item_id)

        media_part = dict(bcur.execute('select * from media_parts where media_item_id=?', (media_item_id,)).fetchone())
        media_part_id = media_part['id']
        media_part['media_item_id'] = new_media_item_id
        fix(media_part, 'directory_id', newdirectories)
        if media_base_subst:
            media_part['file'] = media_part['file'].replace(*media_base_subst)
        new_media_part_id = do_insert('media_parts', media_part, media_part_id)

        media_streams = list(bcur.execute('select * from media_streams where media_item_id=? and media_part_id=?',
                                     (media_item_id, media_part_id)).fetchall())
        for ms in media_streams:
            ms = dict(ms)
            ms['media_item_id'] = new_media_item_id
            ms['media_part_id'] = new_media_part_id
            do_insert('media_streams', ms, ms['id'])

        if mdata_item['metadata_type'] == '1':
            _hash = mdata_item['hash']
            destpath = os.path.join(plex_metadata_base, 'Movies', _hash[0], _hash[1:], 'Contents',
                                    'com.plexapp.agents.imdb')
            pathlib.Path(destpath).mkdir(parents=True, exist_ok=True)


if __name__ == '__main__':
    restore_movies()
    con.commit()
