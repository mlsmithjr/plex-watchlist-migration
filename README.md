# plex-watchlist-migration
Script to migrate your "watch" list of media to a new Plex server.

### This is a first-class quick hack job.  Follow the instructions precisely.

### Tested on Plex Media Server versions:
* 1.11.3.4803
* 1.10.1.4602

*Features*
* Migrate missing users.
* Migrate watch history of users.
* Migrate date added/created to preserve on-deck ordering


This utility is intended to migrate over your user media viewing history when you move to a new
Plex server instance.  The process of copying around the full database folder structure is not reliable and
is just trouble.  So this tool assumes you are starting new libraries on the new machine.
After copying over all your media, instruct the new Plex server to scan and rebuild your metadata.
Yes, this will take quite a bit of time for large collections.  Do this for all your
libraries before proceeding.  This script should be your final step.

After it is done, shut down Plex.

The primary database file for Plex is called **com.plexapp.plugins.library.db** and is located here:

	  Windows: c:/Users/YOUR_USER/AppData/Local/Plex Media Server/Plug-in Support/Databases/com.plexapp.plugins.library.db
	  Linux: /var/lib/plexmediaserver/Library/Application Support/Plex Media Server/Plug-in Support/com.plexapp.plugins.library.db
	  MacOS: /Users/YOUR_USER/Library/Application Support/Plex Media Server/Plug-in Support/com.plexapp.plugins.library.db

### This script is rerun-safe. You can run it as many times as you want and you shouldn't get duplicate data.

1.  Create a working directory somewhere. This is where you will run the script.
1.  Locate the database file of the NEW plex server and copy it to your working directory as "new.db".
1.  Locate the main database file of the OLD plex server and copy it to your working directory as "old.db"
1.  Put this script in the working directory.
1.  In your work directory you should have: export.py (this file), old.db, new.db
1.  At a shell prompt, in the work directory, execute `python3 export.py`
1.  You will probably get a bunch of messages about missing guids.  This is cruft built up in your database over time, usually media you don't have anymore. You can either research them yourself to be sure or continue.
!.  You may rerun the script as often as you like as it will not create duplicate watch records.
1.  Make you have an unaltered backup copy of your new plex database file in case this goes wrong.
1.  Finally, copy new.db over your new plex server's main database file (as com.plexapp.plugins.library.db).
1. Start Plex and verify.  IF you have any concerns just replace the database file with the backup copy you hopefully made.


If anyone wants to contribute to make this script a full blow robust tool I'm all in.
