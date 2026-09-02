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
