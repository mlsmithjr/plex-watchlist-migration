# plex-watchlist-migration
Script to migrate your "watched" list of media to a new Plex server.

## NOTICE (Sep 18 2022)
It seems migrating the watchlist is hit-or-miss these days.  The guid (unique id) for a watched item no longer appears
consistent between source and destination servers.  This basically kills the logic for matching and updating the watched items on the destination. I'm trying to figure out why some shows have different guids between server while others are the same (as it should be).  Until then know that you will be missing some items. They will be listed in the output when you run the tool.

*Features*
* Migrate missing users.
* Migrate watch history of MANAGED users.
* Migrate date added/created to preserve on-deck ordering
* Co-exists with new Plex watchlist sync feature (this tool only handles MANAGED users).

This utility is intended to migrate over your user media viewing history when you move to a new
Plex server instance or to just maintain a failover.  The process of copying around the full database folder structure is not reliable and
is just trouble.  So this tool assumes you are starting new libraries on the new machine.
After copying over all your media, instruct the new Plex server to scan and rebuild your metadata.
Yes, this will take quite a bit of time for large collections.  Do this for all your
libraries before proceeding.  This script should be your final step.

You may start an export on a running Plex server, but you *must* stop the Plex server on the destination side before loading the export.

Export from your primary (source) Plex database:

    python3 watchlist.py -e <source db>  <export file>

    ex: python3 watchlist.py -e /mnt/data/Plex/Library/Application\ Support/Plex\ Media\ Server/Plug-in\ Support/Databases/com.plexapp.plugins.library.db /tmp/export.json


When ready to import into the destination Plex db, *stop the Plex server first* or you may corrupt the database.

    python3 watchlist.py -i <export file>  <destination db>

    ex: python3 watchlist.py -i export.json /mnt/data/Plex/Library/Application\ Support/Plex\ Media\ Server/Plug-in\ Support/Databases/com.plexapp.plugins.library.db


This script is rerun-safe. You can run it as many times as you want, and you shouldn't get duplicate data.

### NOTE: If you get a bunch of warnings during import listing media files not found on the destination it could be:
1. Old media garbage data in the source Plex db.
2. Incompatible _guid_ values between source and destination.  When Plex switched to their own scrapers the guid values changed, which were being used as primary keys in the database. To fix you can try refreshing full metadata of the affected item(s) and re-export.

