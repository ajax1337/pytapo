import logging
import io
import subprocess
import os
import datetime
import tempfile
import aiofiles
from rtp import PayloadType
from .pes import PES

logger = logging.getLogger(__name__)
logging.getLogger("libav").setLevel(logging.ERROR)

PMT_AAC = PES.StreamTypeAAC
PMT_AAC_LATM = 0x11
PMT_MP3_1 = 0x03
PMT_MP3_2 = 0x04
PMT_PCMA = PES.StreamTypePCMATapo
PMT_PCMU = PES.StreamTypePCMUTapo
PMT_VIDEO_TYPES = (0x01, 0x02, 0x10, PES.StreamTypeH264, PES.StreamTypeH265, 0x42)
PMT_AUDIO_TYPES = (PMT_MP3_1, PMT_MP3_2, PMT_AAC, PMT_AAC_LATM, PMT_PCMA, PMT_PCMU)


class Convert:
    def __init__(self):
        self.stream = None
        self.writer = io.BytesIO()
        self.audioWriter = io.BytesIO()
        self.known_lengths = {}
        self.addedChunks = 0
        self.lengthLastCalculatedAtChunk = 0
        self.audio_payload_type = PayloadType.PCMA
        self.audio_sample_rate = 8000

    def _get_audio_format(self):
        if self.audio_payload_type == PayloadType.PCMU:
            return "mulaw"
        return "alaw"

    def _get_audio_rate(self):
        return self.audio_sample_rate

    def _set_audio_properties(self, audio_payload_type=None, sample_rate=None):
        if audio_payload_type is not None:
            self.audio_payload_type = audio_payload_type
            # Default to 16kHz for PCMU (newer firmware), 8kHz otherwise.
            if sample_rate is None:
                self.audio_sample_rate = 16000 if audio_payload_type == PayloadType.PCMU else 8000
        if sample_rate is not None:
            self.audio_sample_rate = sample_rate

    def _scan_ts_audio_stream_type(self, data: bytes):
        pmt_pid = None
        for idx in range(0, min(len(data), 16384), 188):
            packet = data[idx : idx + 188]
            if len(packet) < 188 or packet[0] != 0x47:
                continue
            pid = ((packet[1] & 0x1F) << 8) | packet[2]
            afc = (packet[3] >> 4) & 0x3
            offset = 5 + packet[4] if afc & 0x2 else 4
            payload = packet[offset:]
            if not payload:
                continue
            if pid == 0:
                pointer = payload[0]
                section = payload[1 + pointer :]
                if len(section) >= 12:
                    pmt_pid = ((section[10] & 0x1F) << 8) | section[11]
            elif pmt_pid is not None and pid == pmt_pid:
                pointer = payload[0]
                section = payload[1 + pointer :]
                if len(section) < 12:
                    continue
                prog_info_len = ((section[10] & 0x0F) << 8) | section[11]
                es = section[12 + prog_info_len :]
                pos = 0
                while pos + 5 <= len(es):
                    stream_type = es[pos]
                    if (
                        stream_type not in PMT_VIDEO_TYPES
                        and stream_type in PMT_AUDIO_TYPES
                    ):
                        return stream_type
                    pos += 5 + (((es[pos + 3] & 0x0F) << 8) | es[pos + 4])
                return None
        return None

    def _extract_private_pcm_audio(self, data: bytes):
        pmt_pid = None
        audio_pid = None
        for idx in range(0, len(data), 188):
            packet = data[idx : idx + 188]
            if len(packet) < 188 or packet[0] != 0x47:
                continue
            pid = ((packet[1] & 0x1F) << 8) | packet[2]
            payload = packet[5 + packet[4] :] if (packet[3] >> 4) & 0x2 else packet[4:]
            if not payload:
                continue
            if pid == 0 and len(payload) > 13:
                pointer = payload[0]
                section = payload[1 + pointer :]
                if len(section) > 12:
                    pmt_pid = ((section[10] & 0x1F) << 8) | section[11]
            elif pmt_pid is not None and pid == pmt_pid and audio_pid is None:
                pointer = payload[0]
                section = payload[1 + pointer :]
                if len(section) < 12:
                    continue
                prog_info_len = ((section[10] & 0x0F) << 8) | section[11]
                es = section[12 + prog_info_len :]
                pos = 0
                while pos + 5 <= len(es):
                    stream_type = es[pos]
                    es_pid = ((es[pos + 1] & 0x1F) << 8) | es[pos + 2]
                    es_info_len = ((es[pos + 3] & 0x0F) << 8) | es[pos + 4]
                    if stream_type in (PMT_PCMA, PMT_PCMU):
                        audio_pid = es_pid
                        break
                    pos += 5 + es_info_len
                if audio_pid is not None:
                    break
        if audio_pid is None:
            return None

        out = bytearray()
        current = bytearray()

        def flush_pes(buffer: bytearray):
            if len(buffer) < 9 or buffer[:3] != b"\x00\x00\x01":
                return
            pes_len = (buffer[4] << 8) | buffer[5]
            start = 9 + buffer[8]
            if pes_len > 0:
                out.extend(buffer[start : min(6 + pes_len, len(buffer))])
            else:
                out.extend(buffer[start:])

        for idx in range(0, len(data), 188):
            packet = data[idx : idx + 188]
            if len(packet) < 188 or packet[0] != 0x47:
                continue
            pid = ((packet[1] & 0x1F) << 8) | packet[2]
            if pid != audio_pid:
                continue
            payload_unit_start = (packet[1] >> 6) & 1
            afc = (packet[3] >> 4) & 0x3
            offset = 5 + packet[4] if afc & 0x2 else 4
            payload = packet[offset:]
            if payload_unit_start:
                flush_pes(current)
                current = bytearray(payload)
            else:
                current.extend(payload)
        flush_pes(current)
        return bytes(out) if out else None

    # cuts and saves the video
    async def save(self, fileLocation, fileLength, method="ffmpeg"):
        if method == "ffmpeg":
            tempVideoFileLocation = fileLocation + ".ts"
            tempAudioFileLocation = None
            writer_data = self.writer.getvalue()
            async with aiofiles.open(tempVideoFileLocation, "wb") as file:
                await file.write(writer_data)

            audio_data = self.audioWriter.getvalue()
            audio_stream_type = None
            if not audio_data:
                audio_stream_type = self._scan_ts_audio_stream_type(writer_data)
                if audio_stream_type in (PMT_PCMA, PMT_PCMU):
                    audio_data = self._extract_private_pcm_audio(writer_data) or b""
                    self._set_audio_properties(
                        PayloadType.PCMU
                        if audio_stream_type == PMT_PCMU
                        else PayloadType.PCMA
                    )

            if audio_data:
                audio_format = self._get_audio_format()
                audio_rate = self._get_audio_rate()
                tempAudioFileLocation = f"{fileLocation}.{audio_format}"
                async with aiofiles.open(tempAudioFileLocation, "wb") as file:
                    await file.write(audio_data)
                cmd = [
                    "ffmpeg",
                    "-ss",
                    "00:00:00",
                    "-i",
                    tempVideoFileLocation,
                    "-f",
                    audio_format,
                    "-ar",
                    str(audio_rate),
                    "-i",
                    tempAudioFileLocation,
                    "-t",
                    str(datetime.timedelta(seconds=fileLength)),
                    "-y",
                    "-c:v",
                    "copy",
                    "-c:a",
                    "aac",
                    "-map",
                    "0:v:0",
                    "-map",
                    "1:a:0",
                    fileLocation,
                ]
            elif audio_stream_type in (PMT_AAC, PMT_AAC_LATM):
                cmd = [
                    "ffmpeg",
                    "-ss",
                    "00:00:00",
                    "-i",
                    tempVideoFileLocation,
                    "-t",
                    str(datetime.timedelta(seconds=fileLength)),
                    "-y",
                    "-c",
                    "copy",
                    fileLocation,
                ]
            else:
                cmd = [
                    "ffmpeg",
                    "-ss",
                    "00:00:00",
                    "-i",
                    tempVideoFileLocation,
                    "-t",
                    str(datetime.timedelta(seconds=fileLength)),
                    "-y",
                    "-c:v",
                    "copy",
                    "-an",
                    fileLocation,
                ]
            try:
                result = subprocess.run(
                    cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                )
                if result.returncode != 0:
                    stderr_tail = result.stderr.decode(errors="replace")[-1000:]
                    raise Exception(
                        f"ffmpeg failed with exit code {result.returncode}: {stderr_tail}"
                    )
            except FileNotFoundError:
                raise Exception(
                    "ffmpeg is not installed or not in PATH, it is required to save recordings"
                )
            finally:
                for tempFile in (tempVideoFileLocation, tempAudioFileLocation):
                    if tempFile is None:
                        continue
                    try:
                        os.remove(tempFile)
                    except OSError:
                        pass
        else:
            raise Exception("Method not supported")

    # calculates ideal refresh interval for a real time estimate of downloaded data
    def getRefreshIntervalForLengthEstimate(self):
        if self.addedChunks < 100:
            return 50
        elif self.addedChunks < 1000:
            return 250
        elif self.addedChunks < 10000:
            return 5000
        else:
            return self.addedChunks / 2

    # calculates real stream length, hard on processing since it has to go through all the frames
    def calculateLength(self):
        detectedLength = False
        tmp_name = None
        try:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp_name = tmp.name
                tmp.write(self.writer.getvalue())
                result = subprocess.run(
                    [
                        "ffprobe",
                        "-v",
                        "fatal",
                        "-show_entries",
                        "format=duration",
                        "-of",
                        "default=noprint_wrappers=1:nokey=1",
                        tmp_name,
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )
                if result.returncode != 0:
                    raise Exception(
                        f"ffprobe failed with exit code {result.returncode}: "
                        + result.stdout.decode(errors="replace")[-500:]
                    )
                detectedLength = float(result.stdout)
                self.known_lengths[self.addedChunks] = detectedLength
                self.lengthLastCalculatedAtChunk = self.addedChunks
        except FileNotFoundError:
            logger.warning(
                "ffprobe is not installed or not in PATH, "
                "could not calculate length from stream."
            )
        except Exception as e:
            logger.warning("Could not calculate length from stream: %s", e)
        finally:
            if tmp_name is not None:
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass
        return detectedLength

    # returns length of video, can return an estimate which is usually very close
    def getLength(self, exact=False):
        if bool(self.known_lengths) is True:
            lastKnownChunk = list(self.known_lengths)[-1]
            lastKnownLength = self.known_lengths[lastKnownChunk]
        if (
            exact
            or not self.known_lengths
            or self.addedChunks
            > self.lengthLastCalculatedAtChunk
            + self.getRefreshIntervalForLengthEstimate()
            or lastKnownLength == 0
        ):
            calculatedLength = self.calculateLength()
            if calculatedLength is not False:
                return calculatedLength
            else:
                if bool(self.known_lengths) is True:
                    bytesPerChunk = lastKnownChunk / lastKnownLength
                    return self.addedChunks / bytesPerChunk
        else:
            bytesPerChunk = lastKnownChunk / lastKnownLength
            return self.addedChunks / bytesPerChunk
        return False

    def write(self, data: bytes, audioData: bytes, audioPayloadType=None, audioSampleRate=None):
        self.addedChunks += 1
        self._set_audio_properties(audioPayloadType, audioSampleRate)
        return self.writer.write(data) and self.audioWriter.write(audioData)
