import sqlite3
import sys
import json


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def do_export(source_db, export_filename):
    db_connection = sqlite3.connect(source_db)
    db_connection.row_factory = dict_factory
    cursor = db_connection.cursor()

    #
    # get users
    #
    rows = cursor.execute('select id,name,hashed_password,salt,created_at from accounts where id != 0')
    user_list = []
    for user in rows.fetchall():

        print(f"Exporting for user {user['name']}")
        user_list.append(user)

        #
        # get the watchlist for user
        #
        # Note there is an additional join to metadata_items that seemingly isn't used for anything.  It is actually
        # used implicitly to only grab valid watch records, as there tend to be leftovers from removed media or just
        # orphaned due to bugs.
        #
        user["watchlist"] = []
        watchlist = cursor.execute(
            "select account_id,metadata_item_views.guid,metadata_item_views.metadata_type,metadata_item_views.library_section_id,grandparent_title,"
            "parent_index,parent_title,'index',metadata_item_views.title,thumb_url,viewed_at,grandparent_guid,metadata_item_views.originally_available_at "
            "from metadata_item_views "
            "inner join library_sections on library_sections.id = metadata_item_views.library_section_id "
            "inner join metadata_items on metadata_items.guid = metadata_item_views.guid "
            "where account_id=? and library_sections.section_type in (1,2)", (user["id"],))
        for row in watchlist.fetchall():
            user["watchlist"].append(row)
        print(f"  {len(user['watchlist'])} watched items")

        user["metadata_item_settings"] = {}
        settings = cursor.execute(
            'select account_id,guid,rating,view_offset,view_count,last_viewed_at,created_at,'
            'skip_count,last_skipped_at,changed_at,extra_data '
            'from metadata_item_settings '
            'where account_id = ?', (user["id"],))
        for row in settings.fetchall():
            guid = row["guid"]
            user["metadata_item_settings"][guid] = row
#            user["metadata_item_settings"].append(row)

    #
    # get list of source library sections for mapping to new ones
    #
    source_sections = dict()
    for row in cursor.execute('select id,name from library_sections where section_type in (1,2)').fetchall():
        (_id, name) = row.values()
        source_sections[_id] = {'name': name}

    #
    # get list of media added dates to preserve on-deck ordering in destination system
    #
    ordering = cursor.execute(
        'select guid,added_at,metadata_items.created_at from metadata_items '
        'inner join library_sections on library_sections.id = metadata_items.library_section_id '
        'where library_sections.section_type in (1,2)').fetchall()

    full_export = {"users": user_list, "oldsections": source_sections, "ordering": ordering}
    with open(export_filename, "w") as fp:
        json.dump(full_export, fp, indent=2)
    print("export complete, results in " + export_filename)


def do_import(export_filename, dest_db):
    db_connection = sqlite3.connect(dest_db)
    db_connection.row_factory = dict_factory
    cursor = db_connection.cursor()

    with open(export_filename, "r") as fp:
        source = json.load(fp)

    orig_userlist = source["users"]
    source_sections = source["oldsections"]
    ordering = source["ordering"]

    #
    # Get list of all guids in the new system for validation.
    #
    dest_guid_list = [row["guid"] for row in cursor.execute(
        'select guid from metadata_items mi inner join library_sections ls on mi.library_section_id = ls.id where ls.section_type in (1,2)')]

    #
    # map source to destination sections
    #
    for values in source_sections.values():
        for row in cursor.execute('select id from library_sections where name = ?', (values['name'],)):
            values['newid'] = row["id"]
        if "newid" not in values:
            print('section name not found in database - {}'.format(values['name']))

    #
    # create a map of source to destination user ids
    #
    for orig_user in orig_userlist:
        print(f"Processing user {orig_user['name']}")
        if orig_user["id"] > 1:  # if PlexPass and have non-admin users, bring them over
            # check if user already exists
            cursor.execute('select id, name from accounts where name = ?', (orig_user["name"],))
            existing = cursor.fetchone()
            if existing is None:
                # create user
                cursor.execute('insert into accounts (name,hashed_password,salt,created_at) values (?,?,?,?)',
                               (orig_user["name"], orig_user["hashed_password"], orig_user["salt"],
                                orig_user["created_at"]))
                dest_user_id = cursor.lastrowid
            else:
                # map account id to existing user
                dest_user_id = existing["id"]
        else:
            # probably just the main admin user (account id 1)
            dest_user_id = orig_user["id"]

        #
        # create map of source to destination guid and settings
        #
        source_settings = dict()
        for setting in orig_user["metadata_item_settings"].values():
            guid = setting["guid"]
            source_settings[guid] = setting

        #
        # get list of things the user already watched in the destination so we don't duplicate
        #
        count = 0
        dest_watchlist = cursor.execute(
            "select guid from metadata_item_views inner join library_sections on library_sections.id = metadata_item_views.library_section_id "
            "where account_id=? and library_sections.section_type in (1,2)", (dest_user_id,)).fetchall()

        already_watched = [watched["guid"] for watched in dest_watchlist]

        guid_dedup = []

        #
        # iterate over each watched item from the source system
        #
        for watched in orig_user["watchlist"]:
            guid = watched["guid"]
            gptitle = watched["grandparent_title"]
            ptitle = watched["parent_title"]
            title = watched["title"]

            if guid in guid_dedup:
                # clean up dups in watch list while we are here
                continue

            if guid in already_watched:
                # already been registered, skip
                continue

            if guid not in dest_guid_list:
                print(f'  {gptitle} {ptitle} {title} (guid {guid}) not found in media list - skipped')
                continue

            library_section_id = str(watched["library_section_id"])
            if library_section_id in source_sections.keys():
                if source_sections[library_section_id].get("newid") is not None:
                    library_section_id = source_sections[library_section_id]["newid"]
                else:
                    print('  newid not found in source sections for library section id {}'.format(library_section_id))
                continue
            else:
                print('  unknown library section {} for guid {} - skipped'.format(library_section_id, guid))
                continue

            if guid not in source_settings:
                print(
                    '  Unexpected: guid {} not found in metadata_item_settings for user {}'.format(guid, orig_user["id"]))
                continue
            #
            # everything looks good, insert rows
            #

            # TODO: refactor this; it assumes ordering of values
            w = list(watched.values())
            w[0] = dest_user_id
            w[3] = library_section_id
            watched = tuple(w)

            cursor.execute("insert into metadata_item_views ("
                           "account_id,guid,metadata_type,library_section_id,grandparent_title,"
                           "parent_index,parent_title,'index',title,thumb_url,viewed_at,grandparent_guid,"
                           "originally_available_at) values (?,?,?,?,?,?,?,?,?,?,?,?,?)", watched)

            # TODO: refactor this; it assumes ordering of values
            metadata_item_settings = orig_user["metadata_item_settings"]
            s = list(metadata_item_settings[guid].values())
            s[0] = dest_user_id
            settings = tuple(s)
            cursor.execute(
                'insert into metadata_item_settings (account_id,guid,rating,view_offset,view_count,'
                'last_viewed_at,created_at,skip_count,last_skipped_at,changed_at,extra_data) '
                'values (?,?,?,?,?,?,?,?,?,?,?)', settings)

            guid_dedup.append(guid)
            count += 1

        if count:
            print(f"{count} update(s) for {orig_user['name']}")
        else:
            print(f"No updates for {orig_user['name']}")
    #
    # go through all the media and migrate the added and created dates to preserve "on-deck" ordering
    #

    #
    # This is tricky because Plex uses custom C-based tokenizer code which isn't available to developers.
    # This causes a trigger error when attempting to update metadata_items.
    # This is my workaround: query the triggers, drop the triggers, do the updates, then recreate the triggers.
    # I think this is safe because those triggers just maintain text search indexes of titles, which this
    # code doesn't modify anyhow.
    #

    # grab the triggers
    triggers = cursor.execute("select name,sql from sqlite_master where type='trigger' and tbl_name='metadata_items' and name like '%_update_%'").fetchall()
    # now delete them
    for trigger in triggers:
        cursor.execute(f"drop trigger {trigger['name']}")

    for item in ordering:
        # just do an optimistic update - sqlite will ignore if row doesn't exist
        cursor.execute('update metadata_items set added_at=?, created_at=? where guid=?',
                       (item["added_at"], item["created_at"], item["guid"]))

    # recreate the triggers
    for trigger in triggers:
        cursor.execute(trigger["sql"])

    cursor.close()
    db_connection.commit()


if __name__ == '__main__':
    if len(sys.argv) <= 1:
        print("usage: watchlist.py -e <source database filename> <export filename>")
        print("                    -i <export filename> <destination database filename>")
        sys.exit(0)
    if sys.argv[1] == "-e":
        do_export(sys.argv[2], sys.argv[3])
    if sys.argv[1] == "-i":
        do_import(sys.argv[2], sys.argv[3])
