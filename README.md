# LazyLeech 

<p align="center">
“	Heroku Supported Telegram Torrent Leeching Bot by Some Weebs ” 
</p>


<h2>USE THIS BRANCH ON YOUR RISK, I HAVE ADDED FILE-RENAMING IN TORRENT, MAGNET AND AUTO-DETECT FUNCTIONS IN THIS BRANCH</h2>
<pre>
/filetorrent link | newFileName
/torrent link | newFileName
/ziptorrent link | newFileName
/magnet link | newFileName
/filemagnet link | newFileName
/zipmagnet link | newFileName
link | newFileName (auto detects)
</pre><br>
<h3>Note: I have added serialize renaming for torrents with multiple files</h3>
<pre>
You just need to add {p or P or s or S, any number (optional)} at the end of newFileName
like

/filetorrent link | newFileName {p,3}

well
p/P = prefix
s/S = suffix
and the number stated is range of digits for serialization
default is 3
Thus,

/filetorrent link | test.mkv {p,2} gives
01 test.mkv
02 test.mkv
03 test.mkv
04 test.mkv

/filetorrent link | test.mkv {s,4} gives
test 0001.mkv
test 0002.mkv
test 0003.mkv
test 0004.mkv

/filetorrent link | test.mkv {p} gives
001 test.mkv
002 test.mkv
003 test.mkv
004 test.mkv
</pre>

# Table of Content
- [WHAT IS THIS REPO ABOUT ?](#what-is-this-repo-about)
- [FEATURES](#features)
- [BOT COMMANDS](#bot-commands)
- [TEST THE BOT (DEMO)](https://t.me/joinchat/HC7YmklXMSRPH3N2)
- [CREDITS](#credits-)
- [POINTS TO BE NOTED](#points-to-be-noted)


# What is this repo about?
This is a telegram bot writen with pyrogram for leeching files on the internet to Telegram.

On startup, the bot removes abandoned runtime download, session-download, and
temporary split directories left by a previous process. Telegram thumbnails,
watermarks, Pyrogram sessions, and database records are preserved. Set
`CLEAR_DOWNLOADS_ON_STARTUP=0` to disable this cleanup.

[Bot Demo](https://t.me/joinchat/HC7YmklXMSRPH3N2)

# Features
- Leeching direct download links | Torrent | Magnets.
- Thumbnail and Watermark Support.
- Auto Download from nyaa.si (choose your uploader wisely, try not to add `https://nyaa.si/?page=rss` as your NYAA_RSS_LINK)
- Torrent/Magnet Auto Detect Support.
- Nyaa.si Search Support.
- Upload as zip, streamable, document.
- Can List All your Ongoing Leeches.
- Advanced ytdl
- Docker support.

## Bot Commands

**Leech Module**
```
torrent <Torrent URL or File> or as reply to a Torrent URL or file
ziptorrent <Torrent URL or File> or as reply to a Torrent URL or File
filetorrent <Torrent URL or File> or as reply to a Torrent URL or File - Sends videos as files
magnet <Magnet URL> or as reply to a Magnet URL
zipmagnet <Magnet URL> or as reply to a Magnet URL
filemagnet <Magnet URL> or as reply to a Magnet URL - Sends videos as files
directdl <Direct URL> or as reply to a Direct URL | optional custom file name
direct <Direct URL> or as reply to a Direct URL | optional custom file name
zipdirectdl <Direct URL> or as reply to a Direct URL | optional custom file name
zipdirect <Direct URL> or as reply to a Direct URL | optional custom file name
filedirectdl <Direct URL> or as reply to a Direct URL | optional custom file name - Sends videos as files
filedirect <Direct URL> or as reply to a Direct URL | optional custom file name - Sends videos as files
tera <TeraBox share URL> - Download a TeraBox share and upload it
ziptera <TeraBox share URL> - Download and upload as ZIP
filetera <TeraBox share URL> - Send videos as files
splittera <TeraBox share URL> <size> - Create sequential size-based sessions
splitziptera <TeraBox share URL> <size> - Create ZIP-mode sessions
splitfiletera <TeraBox share URL> <size> - Create force-file sessions
teraintelligent <TeraBox share URL> <workspace> - Plan for source plus split copies
terasessions [page] - List persistent parent chains and child sessions
terasession [session ID] - Show one part or the latest active/recent part
continuetera <chain or session ID> - Resume the next unfinished part
deleteterasession <session ID> - Delete one child session and reindex its chain
deleteterachain <chain ID> - Delete a chain and all child histories
deleteallterasessions - Delete all of your TeraBox histories
setteraboxcookie <ndus value> - Validate and persist a replacement cookie (chat admin)
teraboxcookiestatus - Show cookie source without revealing it (chat admin)
clearteraboxcookie - Remove the database override (chat admin)
queue <URL1> <URL2> ... - Queue links; all Bunkr links form one session
zipqueue <URL1> <URL2> ... - Same queue behavior, uploaded as ZIP
filequeue <URL1> <URL2> ... - Same queue behavior, videos sent as files
splitbunkr <album URL> <files per session> - Split one album into separately resumable sessions
bsessions [page] - List your persistent Bunkr sessions with Previous/Next buttons
bsession <session ID> - List downloaded and unfinished Bunkr file links
pause <session ID> - Pause new/current Bunkr downloading; queued uploads continue
skip <session ID> - Move the active Bunkr file to the bottom and start the next one
continue <session ID> - Resume only unfinished files in a Bunkr session
cancelsession <session ID> - Cancel Bunkr downloading and retain the session
deletesession <session ID> - Delete one session history from the database
deleteallsessions - Delete all of your session histories from the database
cancel - <GID> or as reply to status message
list - Lists your Ongoing Leeches.
```

Each `/queue`, `/zipqueue`, or `/filequeue` invocation combines every valid
Bunkr album/file link it contains into one persistent Bunkr session. Non-Bunkr
links in a mixed queue continue through their normal independent download path.
Deleting a session removes its MongoDB history but does not remove files already
downloaded or waiting in the Telegram upload queue.

`/splitbunkr` extracts an album once and divides its ordered video list into
sessions of the requested maximum size. Part 1 starts immediately; every later
part is stored as paused and starts only when you run `/continue SESSION_ID`.
The command also accepts the size before the URL, or the size alone when replying
to a Bunkr album URL.

Bunkr album downloads track a 20-second rolling speed average and the observed
peak. After a 30-second startup grace period, a file that remains below its
adaptive speed floor for 30 seconds is parked with its partial data intact. The
default absolute floor is 650 KiB/s, peak-relative detection is capped to avoid
overreacting to a short initial burst, and a file may be parked three times.

Fresh files use four range connections. Adaptive recovery steps down from four
to two and then one connection after repeated CDN slowdowns. Only one Bunkr file
may use a CDN host at a time across all sessions, while queued Telegram uploads
continue independently.

Slow CDN health is shared across sessions and persisted in MongoDB in the
`BUNKR_CDN_HEALTH` collection. Its circuit breaker escalates from five to ten to
twenty minutes. Known same-CDN files move behind alternate hosts; when no
alternate exists, the download waits for the cooldown instead of continuing at
the throttled rate. A healthy completed download reduces the CDN's adaptive
level. Tune this behavior with the `BUNKR_SLOW_*`, `BUNKR_CONNECTIONS`,
`BUNKR_RECOVERY_CONNECTIONS`, `BUNKR_MAX_DOWNLOADS_PER_HOST`, and
`BUNKR_MAX_HOST_COOLDOWN_SECONDS` environment variables.

TeraBox downloads use the `TERABOX_COOKIE` (`ndus`) value to resolve a share
through `TERABOX_BASE_URL`. The regional download hop receives that cookie,
but its redirect is handled manually so the cookie is not forwarded to the
storage CDN. The default endpoint is
`https://dm.1024terabox.com/ai/index`. Keep the cookie only in `.env` or your
deployment secret store.

`/splittera URL 40GB` partitions a TeraBox share in its original file order.
Each session contains as many whole files as fit under the requested cumulative
size; a single oversized file receives its own session. `/teraintelligent URL
40GB` treats the size as available workspace instead: files above Telegram's
upload boundary are budgeted at twice their source size because the original
and complete set of numbered split parts coexist temporarily. Once every split
part has been created successfully and queued, the original source is removed;
each accepted part is then removed immediately after its own upload succeeds.
Part 1 starts immediately, and each following part starts automatically only
after all files in the previous part have uploaded successfully. If even one
source-plus-splits peak cannot fit the requested workspace, the command reports
the minimum safe limit and creates no partial chain. Large plans are displayed
ten sessions per page with inline navigation.

`/batchdltera "/My Cloud/Folder" 15GB` is the authenticated-account variant
and is restricted to configured chat administrators. It recursively scans the
named account folder and creates non-overlapping server-side ZIP jobs. A leaf
folder becomes one ZIP when it fits; files directly inside a folder that also
contains subfolders are packed separately; an oversized leaf is divided into
bounded file-ID batches. The generated ZIPs are then grouped using the same
peak-workspace planner, persistent session chain, upload queue, Telegram
splitting, cleanup, resume, pagination, and final linked index as
`/teraintelligent`. Batch URLs are authorized immediately before each download
and are never stored in MongoDB. A one-byte preflight verifies the authorized
stream and its exact length. TeraBox currently omits the standard `bytes` unit
from batch `Content-Range` responses, which makes Aria2 discard its extra
connections. Range-capable account ZIPs therefore use a validated internal
downloader with rolling 8 MiB ranges and up to 16 workers by default; every
response offset and total is checked before it is written. Non-range streams
fall back to one Aria2 connection.
The scan defaults to at most 100 file IDs per generated ZIP, 5,000 directories,
and 100,000 source files; tune these safety limits with
`TERABOX_BATCH_MAX_ITEMS`, `TERABOX_BATCH_MAX_DIRECTORIES`, and
`TERABOX_BATCH_MAX_SOURCE_FILES`. `TERABOX_BATCH_ARCHIVE_OVERHEAD_MB` reserves
extra workspace per generated ZIP (8 MiB by default). Override the ranged-stream
connection count with `TERABOX_BATCH_CONNECTIONS` (1-16).

Per-file `Files:` summaries are suppressed for these session chains. After the
entire chain uploads, the bot sends one folder-style, numbered index containing
the Telegram links for normal files and every `.0001`, `.0002`, ... split part.
Numbered parts from the same source become eligible for upload together and may
finish in any order, subject to `MAX_CONCURRENT_UPLOADS`. Each accepted part is
deleted immediately; the original is deleted only after every part succeeds.
The final index is always sorted into numeric part order even if Telegram
accepted `.0002` before `.0001`.
Each parent chain is stored in `TERABOX_CHAINS`, each child part in
`TERABOX_SESSIONS`, and every source file in `TERABOX_SESSION_FILES` when
`DB_URL` is configured. Parent and child records include a `name` derived from
the shared top-level folder, falling back to the share code for root-level
files. `/terasessions [page]` shows the persistent parent/child hierarchy. Use
`/terasession [ID]` to inspect a part; without an ID it selects the most recently
updated running part, falling back to the most recently updated session when no
part is active. `/continuetera ID` accepts either a chain ID or a child session
ID, so a stopped chain can be resumed after a restart. `/deleteterasession ID`
removes one child and closes the part-number gap, `/deleteterachain ID` removes a
parent and all of its children, and `/deleteallterasessions` removes every
TeraBox chain/session owned by the caller. Deletion removes MongoDB history and
abandoned partial downloads but does not interrupt files already queued for
Telegram upload. Older stored TeraBox sessions are backfilled into parent chain
records when listed. `GB` and `GiB` both use 1024-based units.

Native share chains scan the complete share once when they are created. Every
file's stable `fs_id`, relative path, byte size, source position, and same-site
TeraBox dlink are stored in `TERABOX_SESSION_FILES` and cached in memory for the
life of the bot process. Each download exchanges that stored dlink using the
currently configured cookie, so later parts and post-restart resumes normally
read MongoDB without listing the share again. If TeraBox specifically rejects a
stored dlink as stale, one coroutine refreshes the complete manifest under a
per-chain lock, reconciles all child-session records by `fs_id`, and retries the
authorization once. Account verification errors are not treated as stale
metadata and therefore do not trigger repeated share scans. Legacy records
without an `fs_id` are migrated once by a unique relative-path match.

An administrator can replace an expired cookie in a configured admin chat with
`/setteraboxcookie VALUE`. In groups, the sender must be a Telegram owner or
administrator. The bot attempts to delete that command immediately, validates
the cookie before replacing the old value, and saves the override in the
`TERABOX_CONFIG` MongoDB collection. `/clearteraboxcookie` restores the
environment fallback.

The same resolver can be tested without Telegram:

```powershell
python scripts/terabox_download.py "<TeraBox share URL>" -d downloads/terabox
```

**Other Modules**
```
help - to get organised help message

listrss - List RSS feeds and persistent scheduler state (admin)
addrss <RSS URL> - Add a database-backed RSS feed (admin)
delrss <RSS URL> - Remove a database-backed RSS feed (admin)
pauserss - Persistently pause future RSS feed scans (admin)
resumerss - Resume RSS feed scans (admin)
rssstatus - Show RSS scheduler state and feed counts (admin)

ts - [search query]
nyaa - [search query]
nyaasi - [search query]
sts - [search query]
sukebei - [search query]

thumbnail <as reply to image or as a caption>
setthumbnail <as reply to image or as a caption>
savethumbnail <as reply to image or as a caption>
clearthumbnail
rmthumbnail
removethumbnail
delthumbnail
deletethumbnail

watermark <as reply to image or as a caption>
setwatermark <as reply to image or as a caption>
savewatermark <as reply to image or as a caption>
clearwatermark
rmwatermark
removewatermark
delwatermark
deletewatermark
testwatermark
```

## Credits 📍

[@TheKneesocks](https://t.me/TheKneesocks)

## Points To Be Noted 

- This repo is fork of [Anime Leeching Group](https://t.me/joinchat/BWHQ6lb_FmSP3pxfyYolfg) Bot Leafa-chan.
- I dont own this repo, I have just Uploaded this code on github.
- This Repo is meant for small groups.
- Heroku Supported.
- This Repo is licenced under [AGPL](https://github.com/ShinchanNohara1/Torrent-Bot-Lazyleech/blob/Master/LICENSE) that means you have to share this repo if anyone ask.
