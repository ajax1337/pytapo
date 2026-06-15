import asyncio
import contextlib
import uuid
import aiofiles
import json
import os
import hashlib
from datetime import datetime
from json import JSONDecodeError
from pytapo import Tapo
from .convert import Convert
from ._utils import StreamType
from pytapo.media_stream.error import NonceMissingException


class Downloader:
    FRESH_RECORDING_TIME_SECONDS = 60
    STALL_TIMEOUT_SECONDS = 120
    MAX_NONCE_PRIMES = 2
    PRIME_SLEEP_SECONDS = 3

    def __init__(
        self,
        tapo: Tapo,
        startTime: int,
        endTime: int,
        timeCorrection: int,
        outputDirectory="./",
        padding=None,
        overwriteFiles=None,
        window_size=None,  # affects download speed, with higher values camera sometimes stops sending data
        fileName=None,
        stall_timeout=None,
    ):
        self.tapo = tapo
        self.startTime = startTime
        self.endTime = endTime
        self.padding = padding
        self.fileName = fileName
        self.timeCorrection = timeCorrection
        if padding is None:
            self.padding = 5
        else:
            self.padding = int(padding)

        self.outputDirectory = outputDirectory
        self.overwriteFiles = overwriteFiles
        if window_size is None:
            self.window_size = 200
        else:
            self.window_size = int(window_size)
        self.audio_sample_rate = None
        self.stall_timeout = (
            self.STALL_TIMEOUT_SECONDS if stall_timeout is None else int(stall_timeout)
        )

    async def md5(self, fileName):
        if os.path.isfile(fileName):
            async with aiofiles.open(fileName, "rb") as file:
                contents = await file.read()
            return hashlib.md5(contents).hexdigest()
        return False

    async def _get_audio_sample_rate(self):
        try:
            loop = asyncio.get_event_loop()
            audio_config = await loop.run_in_executor(None, self.tapo.getAudioConfig)
            rate = (
                audio_config.get("audio_config", {})
                .get("microphone", {})
                .get("sampling_rate")
            )
            if rate is None:
                return None
            return int(rate) * 1000
        except Exception:
            return None

    async def downloadFile(self, callbackFunc=None):
        if callbackFunc is not None:
            callbackFunc("Starting download")

        # On NonceMissingException raised while ENTERING the media session
        # (start()/__aenter__ -> AESHelper.from_keyexchange_and_password), NOTHING
        # has been downloaded and NO output file exists yet -- pytapo's start()
        # closes its own writer and sets _started=False before re-raising. So
        # retrying the WHOLE download() from scratch with a fresh generator is
        # safe. We do a best-effort "prime" (open+close a Stream/playback session)
        # + short sleep first, mirroring the production-proven standalone poller,
        # then retry. Bounded by MAX_NONCE_PRIMES; after that the last
        # NonceMissingException is re-raised unchanged.
        #
        # progress["saw"] is updated by _consumeDownload BEFORE any exception can
        # propagate, so the except clause sees it even when the generator raises
        # mid-iteration. We only retry when nothing was yielded yet (the documented
        # session-entry invariant). A nonce that surfaces AFTER a status was yielded
        # (not possible with current pytapo, but a future version might re-handshake
        # mid-stream) re-raises instead of silently re-downloading from scratch.
        primesRemaining = self.MAX_NONCE_PRIMES
        progress = {"saw": False, "status": None}
        while True:
            progress["saw"] = False
            progress["status"] = None
            try:
                await self._consumeDownload(callbackFunc, progress)
                break
            except NonceMissingException:
                if progress["saw"] or primesRemaining <= 0:
                    raise
                primesRemaining -= 1
                await self._primeSession(callbackFunc)

        if callbackFunc is not None:
            callbackFunc("Finished download")

        # "status" is the LAST value yielded by the SUCCESSFUL download() pass.
        # A successful download always yields at least one status dict; guard the
        # degenerate "yielded nothing" case so we never touch an unbound name and
        # never fabricate a phantom-success dict.
        if not progress["saw"]:
            raise RuntimeError(
                "Download produced no status; nothing was downloaded."
            )

        status = progress["status"]
        md5Hash = await self.md5(status["fileName"])

        status["md5"] = "" if md5Hash is False else md5Hash

        return status

    async def _consumeDownload(self, callbackFunc, progress):
        # Consume exactly ONE fresh self.download() async generator, recording
        # progress into the caller-owned "progress" dict AS WE GO (before any
        # exception can propagate), so downloadFile's except clause can tell
        # whether any data was yielded. On the happy path this is behaviourally
        # equivalent to the original "async for status in self.download(): ..."
        # loop. The generator is finalized via contextlib.aclosing as
        # defense-in-depth; on the nonce-at-entry path the generator frame has
        # already unwound (start() closed its own socket), so aclose() is a
        # guarded no-op.
        gen = self.download()
        async with contextlib.aclosing(gen):
            async for status in gen:
                if callbackFunc is not None:
                    callbackFunc(status)
                progress["saw"] = True
                progress["status"] = status

    async def _primeSession(self, callbackFunc=None):
        # Best-effort "prime": open and immediately close a Stream-type media
        # session so the hub/camera warms up its media path, then sleep so the
        # next key exchange returns a nonce. Generic across camera types --
        # getMediaSession(StreamType.Stream, ...) picks the right query_params:
        # hub-storage child -> type="playback" + start_time (the real failing
        # case), childID child -> type="video" (start_time is a harmless no-op
        # for this branch), standalone -> empty/best-effort. Mirrors the proven
        # standalone poller, including using a FRESH playerId for the prime
        # (the reference deliberately uses a uuid distinct from the download's),
        # which we achieve by temporarily swapping self.tapo.playerID around the
        # getMediaSession() call -- still satisfying the "build via
        # getMediaSession" contract. ANY exception here (including a wedged-hub
        # timeout at session entry) is swallowed: priming is advisory; the real
        # retry follows regardless.
        if callbackFunc is not None:
            callbackFunc("Priming session after missing nonce")
        try:
            self.tapo.logger.debugLog(
                "Nonce missing from key exchange; priming media session "
                "before retry."
            )
        except Exception:
            pass
        savedPlayerID = getattr(self.tapo, "playerID", None)
        try:
            try:
                self.tapo.playerID = uuid.uuid4().hex.upper()
            except Exception:
                pass
            primeSession = self.tapo.getMediaSession(
                StreamType.Stream, start_time=str(self.startTime)
            )
            primeSession.set_window_size(50)
            # Bound the prime handshake: session ENTRY is not covered by the
            # download stream's stall_timeout, so a wedged hub here could
            # otherwise block indefinitely.
            if self.stall_timeout and self.stall_timeout > 0:
                await asyncio.wait_for(
                    self._enterAndClosePrime(primeSession),
                    timeout=self.stall_timeout,
                )
            else:
                await self._enterAndClosePrime(primeSession)
        except Exception as primeError:  # noqa: BLE001 - prime is best-effort
            try:
                self.tapo.logger.debugLog(
                    "Prime session before nonce retry failed (ignored): "
                    + repr(primeError)
                )
            except Exception:
                pass
        finally:
            try:
                self.tapo.playerID = savedPlayerID
            except Exception:
                pass
        await asyncio.sleep(self.PRIME_SLEEP_SECONDS)

    @staticmethod
    async def _enterAndClosePrime(primeSession):
        # Open + handshake + immediately close the prime session. async-with
        # guarantees close() runs if __aenter__ succeeds; if __aenter__ (start())
        # raises, pytapo closes its own writer in start()'s except block, so no
        # socket leaks either way.
        async with primeSession:
            pass

    async def download(self, retry=False):
        downloading = True
        while downloading:
            # todo: add a way to not download recent videos to prevent videos in progress
            dateStart = datetime.utcfromtimestamp(int(self.startTime)).strftime(
                "%Y-%m-%d %H_%M_%S"
            )
            dateEnd = datetime.utcfromtimestamp(int(self.endTime)).strftime(
                "%Y-%m-%d %H_%M_%S"
            )
            segmentLength = self.endTime - self.startTime
            if self.fileName is None:
                fileName = (
                    self.outputDirectory + str(dateStart) + "-" + dateEnd + ".mp4"
                )
            else:
                fileName = self.outputDirectory + self.fileName
            if (
                datetime.now().timestamp()
                - self.FRESH_RECORDING_TIME_SECONDS
                - self.timeCorrection
                < self.endTime
            ):
                currentAction = "Recording in progress"
                yield {
                    "currentAction": currentAction,
                    "fileName": fileName,
                    "progress": 0,
                    "total": 0,
                }
                downloading = False
            elif os.path.isfile(fileName):
                currentAction = "Skipping"
                yield {
                    "currentAction": currentAction,
                    "fileName": fileName,
                    "progress": 0,
                    "total": 0,
                }
                downloading = False
            else:
                convert = Convert()
                if self.audio_sample_rate is None:
                    self.audio_sample_rate = await self._get_audio_sample_rate()
                mediaSession = self.tapo.getMediaSession(StreamType.Download)
                if retry:
                    mediaSession.set_window_size(50)
                else:
                    mediaSession.set_window_size(self.window_size)
                async with mediaSession:
                    if getattr(self.tapo, "hubStorageChild", None) is not None:
                        payload = self.tapo.getHubStorageDownloadRequest(
                            self.startTime, self.endTime
                        )
                    else:
                        payload = {
                            "type": "request",
                            "seq": 1,
                            "params": {
                                "playback": {
                                    "client_id": self.tapo.getUserID(),
                                    "channels": [0, 1],
                                    "scale": "1/1",
                                    "start_time": str(self.startTime),
                                    "end_time": str(self.endTime),
                                    "event_type": [1, 2],
                                },
                                "method": "get",
                            },
                        }

                    payload = json.dumps(payload)
                    dataChunks = 0
                    if retry:
                        currentAction = "Retrying"
                    else:
                        currentAction = "Downloading"
                    downloadedFull = False
                    stream = mediaSession.transceive(payload)
                    while True:
                        try:
                            if self.stall_timeout and self.stall_timeout > 0:
                                resp = await asyncio.wait_for(
                                    stream.__anext__(), timeout=self.stall_timeout
                                )
                            else:
                                resp = await stream.__anext__()
                        except StopAsyncIteration:
                            self.tapo.logger.debugLog("Received end of stream.")
                            break
                        except asyncio.TimeoutError:
                            # Camera stopped responding mid-download; break out so we can retry.
                            self.tapo.logger.debugLog(
                                "Timed out waiting for recording data, retrying."
                            )
                            break
                        if resp.mimetype == "video/mp2t":
                            dataChunks += 1
                            convert.write(
                                resp.plaintext,
                                resp.audioPayload,
                                resp.audioPayloadType,
                                self.audio_sample_rate,
                            )
                            detectedLength = convert.getLength()
                            if detectedLength is False:
                                yield {
                                    "currentAction": currentAction,
                                    "fileName": fileName,
                                    "progress": 0,
                                    "total": segmentLength,
                                }
                                detectedLength = 0
                            else:
                                yield {
                                    "currentAction": currentAction,
                                    "fileName": fileName,
                                    "progress": detectedLength,
                                    "total": segmentLength,
                                }
                            if (detectedLength > segmentLength + self.padding) or (
                                retry
                                and detectedLength
                                >= segmentLength  # fix for the latest latest recording
                            ):
                                downloadedFull = True
                                currentAction = "Converting"
                                yield {
                                    "currentAction": currentAction,
                                    "fileName": fileName,
                                    "progress": 0,
                                    "total": 0,
                                }
                                await convert.save(fileName, segmentLength)
                                downloading = False
                                break
                        # in case a finished stream notification is caught, save the chunks as is
                        elif resp.mimetype == "application/json":
                            try:
                                json_data = json.loads(resp.plaintext.decode())
                                if (
                                    json_data.get("type") == "response"
                                    and json_data.get("params", {}).get("error_code", 0)
                                    != 0
                                ):
                                    raise Exception(f"Download failed: {json_data}")

                                if (
                                    "type" in json_data
                                    and json_data["type"] == "notification"
                                    and "params" in json_data
                                    and "event_type" in json_data["params"]
                                    and json_data["params"]["event_type"]
                                    == "stream_status"
                                    and "status" in json_data["params"]
                                    and json_data["params"]["status"] == "finished"
                                ):
                                    self.tapo.logger.debugLog(
                                        "Received json notification about finished stream."
                                    )
                                    downloadedFull = True
                                    currentAction = "Converting"
                                    yield {
                                        "currentAction": currentAction,
                                        "fileName": fileName,
                                        "progress": 0,
                                        "total": 0,
                                    }
                                    await convert.save(fileName, convert.getLength())
                                    downloading = False
                                    break
                            except JSONDecodeError:
                                self.tapo.logger.debugLog(
                                    "Unable to parse JSON sent from device"
                                )
                    if downloading:
                        # Handle case where camera randomly stopped respoding
                        if not downloadedFull and not retry:
                            currentAction = "Retrying"
                            yield {
                                "currentAction": currentAction,
                                "fileName": fileName,
                                "progress": 0,
                                "total": 0,
                            }
                            retry = True
                        else:
                            detectedLength = convert.getLength()
                            if (
                                detectedLength >= segmentLength - 5
                            ):  # workaround for weird cases where the recording is a bit shorter than reported
                                downloadedFull = True
                                currentAction = "Converting [shorter]"
                                yield {
                                    "currentAction": currentAction,
                                    "fileName": fileName,
                                    "progress": 0,
                                    "total": 0,
                                }
                                await convert.save(fileName, segmentLength)
                            else:
                                currentAction = "Giving up"
                                yield {
                                    "currentAction": currentAction,
                                    "fileName": fileName,
                                    "progress": 0,
                                    "total": 0,
                                }
                            downloading = False
