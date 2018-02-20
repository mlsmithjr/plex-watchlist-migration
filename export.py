
#
# This is a first-class quick hack job.  Follow the instructions precisely.
#
# This utility is intended to migrate over your user media viewing history when you move to a new
# Plex server instance.  The process of copying around the full database folder structure is not reliable and
# is just trouble.  So this tool assumes you are starting new libraries on the new machine.
# After copying over all your media, instruct the new Plex server to scan and rebuild your metadata. Do this for all your
# libraries before proceeding.  This script should be your final step.
#
# Yes, this will take quite a bit of time for large collections.
#
# After it is done, shut down Plex.
#
# The primary database file for Plex is called com.plexapp.plugins.library.db and is located here:
#  Windows: c:/Users/YOUR_USER/AppData/Local/Plex Media Server/Plug-in Support/Databases/com.plexapp.plugins.library.db
#  Linux: /var/lib/plexmediaserver/Library/Application Support/Plex Media Server/Plug-in Support/com.plexapp.plugins.library.db
#  MacOS: /Users/YOUR_USER/Library/Application Support/Plex Media Server/Plug-in Support/com.plexapp.plugins.library.db
##
# This script is rerun-safe. You can run it as many times as you want and you shouldn't get duplicate data.
#
# 1. Create a working directory somewhere. This is where you will run the script.
# 2. Locate the database file of the NEW plex server and copy it to your working directory as "new.db".
# 3. Locate the main database file of the OLD plex server and copy it to your working directory as "old.db"
# 4. Put this script in the working directory.
# 5. In your work directory you should have: export.py (this file), old.db, new.db
# 6. At a shell prompt, in the work directory, execute "python3 export.py"
# 7. You will probably get a bunch of messages about missing guids.  This is cruft built up in your database over time,
#    usually media you don't have anymore. You can either research them yourself to be sure or continue.
# 8. Make you have an unaltered backup copy of your new plex database file in case this goes wrong.
# 9. Finally, copy new.db over your new plex server's main database file (as com.plexapp.plugins.library.db).
# 10. Start Plex and verify.  IF you have any concerns just replace the database file with the backup copy you hopefully made.

import sqlite3

oldcon = sqlite3.connect('old.db')
newcon = sqlite3.connect('new.db')
oldcur = oldcon.cursor()
newcur = newcon.cursor()

#
# copy over the users
#
rows = oldcur.execute('select id,name,hashed_password,salt,created_at from accounts')
usermap = []
for row in rows:
    (oldid,name,hashed_password,salt,created_at) = row
    if oldid > 1:       # if PlexPass and have non-admin users, bring them over
        # check if user already exists
        newcur.execute('select id, name from accounts where name = ?', (name,))
        existing = newcur.fetchone()
        if existing is None:
            # create user
            newcur.execute('insert into accounts (name,hashed_password,salt,created_at) values (?,?,?,?)', row[1:])
            newid = newcur.lastrowid
        else:
            # map account id to existing user
            newid = existing[0]
    else:
        # probably just the main admin user (account id 1)
        newid = oldid
    usermap.append((oldid, newid))

#
# Get list of all guids in the new system for validation. Kudos to the Plex team for having the foresight to use
# guids for media.
#
newguidlist = [row[0] for row in newcur.execute('select guid from metadata_items')]

#
# get list of old library sections for mapping to new ones
#
oldsections = dict()
for row in oldcur.execute('select id,name from library_sections'):
    (id,name) = row
    oldsections[id] = { 'name': name }

#
# map to new sections
#
for id,values in oldsections.items():
    for row in newcur.execute('select id from library_sections where name = ?', (values['name'],)):
        values['newid'] = row[0]

#
# get list of things the users have watched
#

count = 0
for user in usermap:
    (oldid, newid) = user

    settings = oldcur.execute(
        'select account_id,guid,rating,view_offset,view_count,last_viewed_at,created_at,'
        'skip_count,last_skipped_at,changed_at,extra_data '
        'from metadata_item_settings '
        'where account_id = ?', (oldid,))
    oldsettings = dict()
    for setting in settings:
        guid = setting[1]
        oldsettings[guid] = setting

    newwatchlist = newcur.execute(
        "select guid from metadata_item_views where account_id=? and library_section_id is not null", (newid,))

    #
    # make a list of already watched items in the new database, in case this script is being rerun we don't want duplicates
    #
    already_watched = []
    for newwatch in newwatchlist:
        already_watched.append(newwatch[0])

    watchlist = oldcur.execute(
        "select account_id,guid,metadata_type,library_section_id,grandparent_title,"
        "parent_index,parent_title,'index',title,thumb_url,viewed_at,grandparent_guid,originally_available_at "
        "from metadata_item_views where account_id=? and library_section_id is not null", (oldid,))

    guid_dedup = []

    for watched in watchlist:
        guid = watched[1]

        if guid in guid_dedup:
            # clean up dups in watch list while we are here
            continue

        if guid in already_watched:
            # already been registered, skip
            continue

        if not guid in newguidlist:
            print('guid ' + guid + ' not found in media list - skipped')
            continue

        library_section_id = watched[3]
        if library_section_id in oldsections.keys():
            library_section_id = oldsections[library_section_id]['newid']
        else:
            print('unknown library section {} for guid {} - skipped'.format(library_section_id, guid))
            continue

        if not guid in oldsettings:
            print('Unexpected: guid {} not found in metadata_item_settings for user {}'.format(guid, oldid))
            continue

        #
        # everything looks good, insert rows
        #
        w = list(watched)
        w[0] = newid
        w[3] = library_section_id
        watched = tuple(w)
        newcur.execute("insert into metadata_item_views ("
                "account_id,guid,metadata_type,library_section_id,grandparent_title,"
                "parent_index,parent_title,'index',title,thumb_url,viewed_at,grandparent_guid,"
                "originally_available_at) values (?,?,?,?,?,?,?,?,?,?,?,?,?)", watched)

        s = list(oldsettings[guid])
        s[0] = newid
        settings = tuple(s)
        newcur.execute(
            'insert into metadata_item_settings (account_id,guid,rating,view_offset,view_count,'
            'last_viewed_at,created_at,skip_count,last_skipped_at,changed_at,extra_data) '
            'values (?,?,?,?,?,?,?,?,?,?,?)', settings)

        guid_dedup.append(guid)
        count += 1

#
# go through all the media and migrate the added and created dates to preserve "on-deck" ordering
#

for item in oldcur.execute('select guid,added_at,created_at from metadata_items'):
    (guid,added_at,created_at) = item
    # just do an optimistic update - sqllite will ignore if row doesn't exist
    newcur.execute('update metadata_items set added_at=?, created_at=? where guid=?', (added_at,created_at,guid))


oldcur.close()
newcur.close()
oldcon.commit()
newcon.commit()
print('migrated {} watch records for {} users'.format(count, len(usermap)))
