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
bsessions - List your persistent Bunkr sessions
bsession <session ID> - List downloaded and unfinished Bunkr file links
pause <session ID> - Pause new/current Bunkr downloading; queued uploads continue
skip <session ID> - Move the active Bunkr file to the bottom and start the next one
continue <session ID> - Resume only unfinished files in a Bunkr session
cancelsession <session ID> - Cancel Bunkr downloading and retain the session
deletesession <session ID> - Delete a Bunkr session and its stored link history
deleteallsessions - Delete all of your stored Bunkr session histories
cancel - <GID> or as reply to status message
list - Lists your Ongoing Leeches.
```

Bunkr album downloads automatically move a persistently slow file to the bottom
of its session queue. By default this happens below 128 KiB/s for 90 continuous
seconds after a 60-second startup grace period, at most twice per file. The
thresholds can be adjusted with the `BUNKR_SLOW_*` environment variables.
Fresh Bunkr downloads use four range connections by default. A deferred file
keeps its partial data and retries with one connection, which is less likely to
remain CDN-throttled. Downloads are also serialized per Bunkr CDN host, while
different hosts and queued Telegram uploads can continue independently. Tune
this with `BUNKR_CONNECTIONS`, `BUNKR_RECOVERY_CONNECTIONS`, and
`BUNKR_MAX_DOWNLOADS_PER_HOST`.

**Other Modules**
```
help - to get organised help message

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
