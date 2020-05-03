
import sqlite3
import os
import shutil

plex_db_path = 'com.plexapp.plugins.library.db'
con = sqlite3.connect(plex_db_path)
con.row_factory = sqlite3.Row
cur = con.cursor()
section_name = 'Mark'
plex_metadata_base = '/mnt/b/plex/Metadata/'
media_base_subst = ('/Volumes/merger/media/video', '/mnt/b/media/video')


def subst_name(name: str):
    return name.replace(*media_base_subst)


def fetch_info(full_hash: str):
    prefix = full_hash[0]
    hash = full_hash[1:]
    content_dir = os.path.join(plex_metadata_base, 'Movies', prefix, hash + '.bundle', 'Contents',
                               'com.plexapp.agents.imdb')
    try:
        with open(os.path.join(content_dir, 'Info.xml'), 'r') as info_file:
            return info_file.read()
    except:
        return None


def copy_poster(full_hash: str, url: str, media_filename: str):
    if not url.startswith('metadata://'):
        return False
    prefix = full_hash[0]
    hash = full_hash[1:]
    content_dir = os.path.join(plex_metadata_base, 'Movies', prefix, hash + '.bundle', 'Contents',
                               'com.plexapp.agents.imdb', 'posters')
    filename = url.split('_')[1]
    filepath = os.path.join(content_dir, filename)
    if os.path.exists(filepath):
        shutil.copy(filepath, subst_name(media_filename) + '-poster.jpg')
        return True
    return False


def copy_art(full_hash: str, url: str, media_filename: str):
    if not url.startswith('metadata://'):
        return False
    prefix = full_hash[0]
    hash = full_hash[1:]
    content_dir = os.path.join(plex_metadata_base, 'Movies', prefix, hash + '.bundle', 'Contents',
                               'com.plexapp.agents.imdb', 'art')
    filename = url.split('_')[1]
    filepath = os.path.join(content_dir, filename)
    if os.path.exists(filepath):
        shutil.copy(filepath, subst_name(media_filename) + '-art.jpg')
        return True
    return False

def rows_to_list(rows):
    return [row_to_dict(row) for row in rows]


def row_to_dict(row: sqlite3.Row):
    return dict(row)


def save_movies():

    global_file = dict()
    result = cur.execute('select * from library_sections where section_type in (1,2)')
    sections = result.fetchall()
    global_file['library_sections'] = rows_to_list(sections)

    global_file['section_locations'] = rows_to_list(cur.execute('select * from section_locations').fetchall())
    global_file['directories'] = rows_to_list(cur.execute('select * from directories').fetchall())
    global_file['tags'] = rows_to_list(cur.execute('select * from tags').fetchall())

    for section in sections:
        if section_name and section['name'] != section_name:
            continue

        tmp_cur = con.cursor()
        for row in cur.execute('select * from media_items where library_section_id=?', (section['id'],)).fetchall():
            stack = dict()
            part = tmp_cur.execute('select * from media_parts where media_item_id=?', (row['id'],)).fetchone()
            stack['media_parts'] = row_to_dict(part)
            media_streams = tmp_cur.execute('select * from media_streams where media_part_id=?', (part['id'],)).fetchall()
            stack['media_streams'] = rows_to_list(media_streams)
            metadata_item = cur.execute('select * from metadata_items where id = ?', (row['metadata_item_id'],)).fetchone()
            stack['metadata_items'] = row_to_dict(metadata_item)

            #
            # copy over the physical, separate items
            #
            file = subst_name(part['file'])

            info = fetch_info(metadata_item['hash'])
            if info is None:
                continue

            print(file)
            with open(file + '-info.xml', 'w') as f:
                f.write(info)
            if not copy_poster(metadata_item['hash'], metadata_item['user_thumb_url'], part['file']):
                print(f'poster for {file} not copied')
            if not copy_art(metadata_item['hash'], metadata_item['user_art_url'], part['file']):
                print(f'art for {file} not copied')

        tmp_cur.close()


if __name__ == '__main__':
    save_movies()
