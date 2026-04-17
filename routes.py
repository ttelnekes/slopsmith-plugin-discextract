"""Base Game Song Extractor plugin — split songs.psarc into individual CDLCs."""

import asyncio
import importlib.util
import json
import os
from pathlib import Path

from fastapi import WebSocket, WebSocketDisconnect

_get_dlc_dir = None
_extract_meta = None
_meta_db = None
_extractor_mod = None


def _extractor():
    """Load this plugin's extractor.py with a unique module name to avoid conflicts."""
    global _extractor_mod
    if _extractor_mod is None:
        spec = importlib.util.spec_from_file_location(
            "disc_extract_extractor", Path(__file__).parent / "extractor.py"
        )
        _extractor_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(_extractor_mod)
    return _extractor_mod


def _find_rs_dir(dlc_dir):
    """Find the Rocksmith install directory from the DLC path."""
    # Check Docker mount first
    if Path("/rocksmith/songs.psarc").exists():
        return Path("/rocksmith")
    # DLC is usually at .../Rocksmith2014/dlc
    if dlc_dir and dlc_dir.parent.name == "Rocksmith2014":
        return dlc_dir.parent
    # Try common locations
    common = [
        Path.home() / ".local/share/Steam/steamapps/common/Rocksmith2014",
        Path.home() / ".steam/steam/steamapps/common/Rocksmith2014",
        Path("C:/Program Files (x86)/Steam/steamapps/common/Rocksmith2014"),
        Path("C:/Program Files/Steam/steamapps/common/Rocksmith2014"),
    ]
    for p in common:
        if p.exists():
            return p
    return None


def setup(app, context):
    global _get_dlc_dir, _extract_meta, _meta_db
    _get_dlc_dir = context["get_dlc_dir"]
    _extract_meta = context["extract_meta"]
    _meta_db = context["meta_db"]

    @app.get("/api/plugins/disc_extract/status")
    def disc_status():
        """Check if songs.psarc is available and list contained songs."""
        dlc = _get_dlc_dir()
        if not dlc:
            return {"error": "DLC folder not configured"}

        rs_dir = _find_rs_dir(dlc)
        songs_psarc = rs_dir / "songs.psarc" if rs_dir else None
        has_songs_psarc = songs_psarc and songs_psarc.exists()

        if not has_songs_psarc:
            return {
                "has_songs_psarc": False,
                "rs_dir": str(rs_dir) if rs_dir else None,
                "songs": [],
            }

        # Read song list from songs.psarc manifests
        songs = {}
        try:
            from psarc import read_psarc_entries
            files = read_psarc_entries(str(songs_psarc), ["manifests/songs/*.json"])
            for p, data in files.items():
                if not p.endswith(".json"):
                    continue
                j = json.loads(data)
                for k, v in j.get("Entries", {}).items():
                    attrs = v.get("Attributes", {})
                    sk = attrs.get("SongKey", "")
                    sn = attrs.get("SongName", "")
                    sa = attrs.get("ArtistName", "")
                    arr = attrs.get("ArrangementName", "")
                    if sk and sn and arr not in ("Vocals", "ShowLights", "JVocals"):
                        if sk not in songs:
                            songs[sk] = {"key": sk, "title": sn, "artist": sa, "arrangements": []}
                        songs[sk]["arrangements"].append(arr)
        except Exception as e:
            return {
                "has_songs_psarc": True,
                "rs_dir": str(rs_dir),
                "error": str(e),
                "songs": [],
            }

        # Check which are already extracted
        extracted = set()
        for f in dlc.iterdir():
            if f.name.endswith("_p.psarc"):
                extracted.add(f.name)

        song_list = sorted(songs.values(), key=lambda s: s["title"])
        for s in song_list:
            sanitize_filename = _extractor().sanitize_filename
            out_name = f"{sanitize_filename(s['title'])} - {sanitize_filename(s['artist'])}_p.psarc"
            s["extracted"] = out_name in extracted

        return {
            "has_songs_psarc": True,
            "rs_dir": str(rs_dir),
            "song_count": len(songs),
            "songs": song_list,
            "extracted_count": sum(1 for s in song_list if s["extracted"]),
        }

    @app.websocket("/ws/plugins/disc_extract/extract")
    async def ws_extract(websocket: WebSocket):
        """Extract base game songs with progress."""
        await websocket.accept()

        dlc = _get_dlc_dir()
        if not dlc:
            await websocket.send_json({"error": "DLC folder not configured"})
            await websocket.close()
            return

        rs_dir = _find_rs_dir(dlc)
        if not rs_dir or not (rs_dir / "songs.psarc").exists():
            await websocket.send_json({"error": "songs.psarc not found"})
            await websocket.close()
            return

        progress_queue = asyncio.Queue()

        def _do_extract():
            try:
                _ext = _extractor()
                PsarcReader = _ext.PsarcReader
                parse_bnk_wem_id = _ext.parse_bnk_wem_id
                get_song_info = _ext.get_song_info
                sanitize_filename = _ext.sanitize_filename
                build_hsan = _ext.build_hsan
                build_aggregate_graph = _ext.build_aggregate_graph
                update_xblock = _ext.update_xblock
                MANIFEST_DIR = _ext.MANIFEST_DIR
                APPID = _ext.APPID
                from patcher import pack_psarc
                import tempfile

                songs_psarc = rs_dir / "songs.psarc"
                reader = PsarcReader(str(songs_psarc))

                # Get song list from xblocks
                xblock_files = {
                    name: name.rsplit("/", 1)[-1]
                    for name in reader.list_files()
                    if name.startswith("gamexblocks/nsongs/") and name.endswith(".xblock")
                }
                song_keys = [(fname.replace(".xblock", ""), path, fname)
                             for path, fname in sorted(xblock_files.items())]

                # Load shared resources
                flat_root = reader.get("flatmodels/rs/rsenumerable_root.flat")
                flat_song = reader.get("flatmodels/rs/rsenumerable_song.flat")
                hsan_path = f"manifests/{MANIFEST_DIR}/{MANIFEST_DIR}.hsan"
                hsan_raw = reader.get(hsan_path)
                hsan_data = json.loads(hsan_raw) if hsan_raw else {"Entries": {}}
                hsan_entries = hsan_data.get("Entries", {})

                extracted = 0
                total = len(song_keys)

                for i, (key, xblock_path, xblock_fname) in enumerate(song_keys):
                    pct = int(5 + (i / max(total, 1)) * 90)
                    manifests = reader.get_matching([f"manifests/{MANIFEST_DIR}/{key}_*.json"])
                    sngs = reader.get_matching([f"songs/bin/generic/{key}_*.sng"])
                    album_art = reader.get_matching([f"gfxassets/album_art/album_{key}_*.dds"])
                    showlights = reader.get(f"songs/arr/{key}_showlights.xml")
                    xblock_data = reader.get(xblock_path)

                    if not manifests:
                        continue

                    info = None
                    for mpath, mdata in sorted(manifests.items()):
                        if "_vocals" not in mpath:
                            info = get_song_info(mdata)
                            break
                    if not info:
                        info = get_song_info(list(manifests.values())[0])

                    progress_queue.put_nowait({
                        "stage": f"[{i+1}/{total}] {info['artist']} - {info['title']}",
                        "progress": pct,
                    })

                    artist = sanitize_filename(info["artist"])
                    title = sanitize_filename(info["title"])
                    out_name = f"{title} - {artist}_p.psarc"

                    # Skip if already extracted
                    if (dlc / out_name).exists():
                        extracted += 1
                        continue

                    # Audio (self-contained in songs.psarc)
                    song_bnk_name = f"audio/windows/song_{key}.bnk"
                    preview_bnk_name = f"audio/windows/song_{key}_preview.bnk"
                    song_bnk = reader.get(song_bnk_name)
                    preview_bnk = reader.get(preview_bnk_name)

                    if not song_bnk:
                        continue

                    song_wem_id = parse_bnk_wem_id(song_bnk)
                    preview_wem_id = parse_bnk_wem_id(preview_bnk) if preview_bnk else None

                    wem_files = {}
                    if song_wem_id:
                        wem_name = f"audio/windows/{song_wem_id}.wem"
                        wem_data = reader.get(wem_name)
                        if wem_data:
                            wem_files[wem_name] = wem_data

                    if preview_wem_id and preview_wem_id != song_wem_id:
                        wem_name = f"audio/windows/{preview_wem_id}.wem"
                        wem_data = reader.get(wem_name)
                        if wem_data:
                            wem_files[wem_name] = wem_data

                    if not wem_files:
                        continue

                    with tempfile.TemporaryDirectory() as tmpdir:
                        tmpdir = Path(tmpdir)
                        dlc_key = f"songs_dlc_{key}"

                        (tmpdir / "appid.appid").write_text(APPID)

                        audio_dir = tmpdir / "audio" / "windows"
                        audio_dir.mkdir(parents=True)
                        (audio_dir / f"song_{key}.bnk").write_bytes(song_bnk)
                        if preview_bnk:
                            (audio_dir / f"song_{key}_preview.bnk").write_bytes(preview_bnk)
                        for wem_path, wem_data in wem_files.items():
                            (audio_dir / wem_path.rsplit("/", 1)[-1]).write_bytes(wem_data)

                        flat_dir = tmpdir / "flatmodels" / "rs"
                        flat_dir.mkdir(parents=True)
                        if flat_root:
                            (flat_dir / "rsenumerable_root.flat").write_bytes(flat_root)
                        if flat_song:
                            (flat_dir / "rsenumerable_song.flat").write_bytes(flat_song)

                        xblock_dir = tmpdir / "gamexblocks" / "nsongs"
                        xblock_dir.mkdir(parents=True)
                        (xblock_dir / xblock_fname).write_bytes(
                            update_xblock(xblock_data, MANIFEST_DIR, dlc_key))

                        art_dir = tmpdir / "gfxassets" / "album_art"
                        art_dir.mkdir(parents=True)
                        for art_path, art_data in album_art.items():
                            (art_dir / art_path.rsplit("/", 1)[-1]).write_bytes(art_data)

                        manifest_out_dir = tmpdir / "manifests" / dlc_key
                        manifest_out_dir.mkdir(parents=True)
                        manifest_names = []
                        for mpath, mdata in manifests.items():
                            mfname = mpath.rsplit("/", 1)[-1]
                            manifest_names.append(mfname.replace(".json", ""))
                            (manifest_out_dir / mfname).write_bytes(mdata)

                        (manifest_out_dir / f"{dlc_key}.hsan").write_text(
                            build_hsan(manifests, hsan_entries, key))

                        arr_dir = tmpdir / "songs" / "arr"
                        arr_dir.mkdir(parents=True)
                        if showlights:
                            (arr_dir / f"{key}_showlights.xml").write_bytes(showlights)

                        sng_dir = tmpdir / "songs" / "bin" / "generic"
                        sng_dir.mkdir(parents=True)
                        sng_names = []
                        for sng_path, sng_data in sngs.items():
                            sng_fname = sng_path.rsplit("/", 1)[-1]
                            sng_names.append(sng_fname.replace(".sng", ""))
                            (sng_dir / sng_fname).write_bytes(sng_data)

                        dds_names = [p.rsplit("/", 1)[-1].replace(".dds", "") for p in album_art]

                        (tmpdir / f"{key}_aggregategraph.nt").write_text(
                            build_aggregate_graph(key, manifest_names, sng_names, dds_names,
                                                  showlights is not None, xblock_fname))

                        pack_psarc(tmpdir, dlc / out_name)
                        extracted += 1

                reader.close()

                # Trigger library rescan for new files
                for f in dlc.iterdir():
                    if f.name.endswith("_p.psarc"):
                        stat = f.stat()
                        existing = _meta_db.get(f.name, stat.st_mtime, stat.st_size)
                        if not existing:
                            try:
                                meta = _extract_meta(f)
                                _meta_db.put(f.name, stat.st_mtime, stat.st_size, meta)
                            except Exception:
                                pass

                progress_queue.put_nowait({
                    "done": True,
                    "progress": 100,
                    "total": extracted,
                })

            except Exception as e:
                import traceback
                traceback.print_exc()
                progress_queue.put_nowait({"error": str(e)})

        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _do_extract)

        try:
            while True:
                try:
                    msg = await asyncio.wait_for(progress_queue.get(), timeout=2.0)
                    await websocket.send_json(msg)
                    if msg.get("done") or msg.get("error"):
                        break
                except asyncio.TimeoutError:
                    if task.done():
                        break
        except WebSocketDisconnect:
            pass

        await websocket.close()
