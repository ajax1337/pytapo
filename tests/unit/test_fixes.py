"""Unit tests for bug fixes. These run without a camera."""

import asyncio
import inspect
import subprocess
from unittest import mock

import pytest

from pytapo import Tapo
from pytapo.media_stream._utils import (
    check_and_correct_http_response,
    index_from,
    parse_http_response,
)
from pytapo.media_stream.convert import Convert
from pytapo.media_stream.tsReader import TSReader


class TestIndexFrom:
    def test_returns_actual_index_not_bool(self):
        # regression: walrus precedence made this return True (== 1)
        assert index_from(b"abcdefabc", b"abc", 1) == 6

    def test_returns_minus_one_when_not_found(self):
        assert index_from(b"abcdef", b"xyz", 1) == -1

    def test_start_index_zero_uses_plain_find(self):
        assert index_from(b"abcdef", b"cd", 0) == 2

    def test_start_index_beyond_length(self):
        assert index_from(b"abc", b"a", 10) == -1


class TestCheckAndCorrectHttpResponse:
    def test_clean_response_passes_through(self):
        data = b"HTTP/1.1 200 OK"
        assert check_and_correct_http_response(data) == data

    def test_garbage_prefix_is_stripped(self):
        data = b"HTTP ERROR 401HTTP/1.0 200 OK"
        assert check_and_correct_http_response(data) == b"HTTP/1.0 200 OK"

    def test_no_http_marker_returns_input(self):
        # regression: used to fall through and return None
        data = b"complete garbage"
        assert check_and_correct_http_response(data) == data


class TestParseHttpResponse:
    def test_parses_status_code(self):
        http_ver, status_code, status = parse_http_response(b"HTTP/1.1 200 OK")
        assert http_ver == b"HTTP/1.1"
        assert status_code == 200
        assert status == b"OK"


class TestTSReaderSync:
    def test_sync_resets_end_position_to_packet_size(self):
        # regression: sync() assigned self.s = self (the object itself)
        reader = TSReader()
        packet = bytearray([0x47] + [0] * 187)
        reader.setBuffer(packet + packet)
        reader.i = 188  # simulate a fully consumed first packet
        assert reader.sync() is True
        assert reader.s == reader.PacketSize
        assert reader.left() == reader.PacketSize  # would TypeError before fix


class TestMutableDefaults:
    def test_no_shared_mutable_defaults_in_tapo(self):
        from pytapo import Tapo

        for name, fn in inspect.getmembers(Tapo, inspect.isfunction):
            for param in inspect.signature(fn).parameters.values():
                assert not isinstance(
                    param.default, (list, dict, set)
                ), f"Tapo.{name} has mutable default for {param.name}"

    def test_no_shared_mutable_defaults_in_media_stream(self):
        from pytapo.media_stream.downloaderv2 import DownloaderV2
        from pytapo.media_stream.session import HttpMediaSession
        from pytapo.media_stream.streamer import Streamer

        for cls in (DownloaderV2, HttpMediaSession, Streamer):
            for name, fn in inspect.getmembers(cls, inspect.isfunction):
                for param in inspect.signature(fn).parameters.values():
                    assert not isinstance(
                        param.default, (list, dict, set)
                    ), f"{cls.__name__}.{name} has mutable default for {param.name}"


class TestKlapTransport:
    def test_get_encryption_method_is_instance_method(self):
        # regression: getEncryptionMethod() was missing self entirely
        from pytapo.transport.klap.klap import Klap
        from pytapo.const import EncryptionMethod

        klap = Klap("127.0.0.1", 443, "user", "pass")
        assert klap.getEncryptionMethod() == EncryptionMethod.SHA256

    def test_transport_style_unbound_call(self):
        # Transport calls backend methods as Class.method(self, ...)
        from pytapo.transport.klap.klap import Klap
        from pytapo.const import EncryptionMethod

        klap = Klap("127.0.0.1", 443, "user", "pass")
        assert Klap.getEncryptionMethod(klap) == EncryptionMethod.SHA256

    def test_constructor_does_not_print(self, capsys):
        from pytapo.transport.klap.klap import Klap

        Klap("127.0.0.1", 443, "user", "pass")
        assert capsys.readouterr().out == ""


class TestConvertSave:
    def _run_save(self, tmp_path, run_mock):
        convert = Convert()
        convert.writer.write(b"\x47" * 188)
        convert.audioWriter.write(b"\x00" * 160)
        out_file = str(tmp_path / "out with spaces.mp4")
        with mock.patch(
            "pytapo.media_stream.convert.subprocess.run", run_mock
        ):
            asyncio.run(convert.save(out_file, 1))
        return out_file

    def test_invokes_ffmpeg_without_shell(self, tmp_path):
        run_mock = mock.Mock(
            return_value=subprocess.CompletedProcess([], 0, stdout=b"", stderr=b"")
        )
        out_file = self._run_save(tmp_path, run_mock)
        cmd = run_mock.call_args[0][0]
        assert isinstance(cmd, list)  # regression: was a shell string via os.system
        assert cmd[0] == "ffmpeg"
        assert out_file in cmd  # path with spaces passed as a single argv element

    def test_raises_clear_error_when_ffmpeg_missing(self, tmp_path):
        run_mock = mock.Mock(side_effect=FileNotFoundError())
        with pytest.raises(Exception, match="ffmpeg is not installed"):
            self._run_save(tmp_path, run_mock)

    def test_raises_on_nonzero_exit(self, tmp_path):
        run_mock = mock.Mock(
            return_value=subprocess.CompletedProcess(
                [], 1, stdout=b"", stderr=b"boom"
            )
        )
        with pytest.raises(Exception, match="exit code 1"):
            self._run_save(tmp_path, run_mock)

    def test_temp_files_removed_on_failure(self, tmp_path):
        run_mock = mock.Mock(
            return_value=subprocess.CompletedProcess(
                [], 1, stdout=b"", stderr=b"boom"
            )
        )
        with pytest.raises(Exception):
            self._run_save(tmp_path, run_mock)
        leftovers = [p.name for p in tmp_path.iterdir()]
        assert leftovers == []

    def test_saves_video_only_when_no_audio_payload_exists(self, tmp_path):
        convert = Convert()
        convert.writer.write(b"\x47" * 188)
        out_file = str(tmp_path / "out.mp4")
        run_mock = mock.Mock(
            return_value=subprocess.CompletedProcess([], 0, stdout=b"", stderr=b"")
        )
        with mock.patch("pytapo.media_stream.convert.subprocess.run", run_mock):
            asyncio.run(convert.save(out_file, 1))
        cmd = run_mock.call_args[0][0]
        assert "-an" in cmd
        assert all(not arg.endswith((".alaw", ".mulaw")) for arg in cmd)


class TestConvertCalculateLength:
    def test_missing_ffprobe_returns_false(self):
        convert = Convert()
        with mock.patch(
            "pytapo.media_stream.convert.subprocess.run",
            mock.Mock(side_effect=FileNotFoundError()),
        ):
            assert convert.calculateLength() is False

    def test_ffprobe_failure_returns_false(self):
        convert = Convert()
        with mock.patch(
            "pytapo.media_stream.convert.subprocess.run",
            mock.Mock(
                return_value=subprocess.CompletedProcess([], 1, stdout=b"err")
            ),
        ):
            assert convert.calculateLength() is False

    def test_ffprobe_success_returns_duration(self):
        convert = Convert()
        with mock.patch(
            "pytapo.media_stream.convert.subprocess.run",
            mock.Mock(
                return_value=subprocess.CompletedProcess([], 0, stdout=b"12.5\n")
            ),
        ):
            assert convert.calculateLength() == 12.5


class TestHubStorageChild:
    def _make_tapo(self):
        tapo = object.__new__(Tapo)
        tapo.childID = "child-device-id"
        tapo.playerID = "PLAYER"
        tapo.hubStorageChild = {
            "alias": "Future Hub Camera",
            "device_id": "child-device-id",
            "device_model": "C999",
            "device_type": "SMART.IPCAMERA",
            "hub_storage_enabled": True,
            "mac": "AABBCCDDEEFF",
            "network_mode": "wireless",
        }
        tapo.logger = mock.Mock()
        return tapo

    def test_hub_storage_recording_query_uses_child_identifiers(self):
        tapo = self._make_tapo()
        tapo.executeFunction = mock.Mock(
            return_value={"playback": {"search_video_results": []}}
        )

        tapo.getRecordingsUTC(1781460517, 1781460529, 3, 4)

        method, params = tapo.executeFunction.call_args[0]
        assert method == "searchVideoWithUTC"
        query = params["playback"]["search_video_with_utc"]
        assert query["child_device_id"] == "child-device-id"
        assert query["child_device_mac"] == "AABBCCDDEEFF"
        assert query["player_id"] == "PLAYER"
        assert "id" not in query

    def test_hub_storage_download_request_uses_download_payload(self):
        tapo = self._make_tapo()

        payload = tapo.getHubStorageDownloadRequest(1781460517, 1781460529)

        assert payload["params"]["method"] == "get"
        assert "download" in payload["params"]
        download = payload["params"]["download"]
        assert download["dev_id"] == "child-device-id"
        assert download["mac"] == "AABBCCDDEEFF"
        assert download["player_id"] == "PLAYER"
        assert download["client_id"] == 1

    def test_finds_any_enabled_hub_storage_camera(self):
        tapo = object.__new__(Tapo)
        tapo.getGeneralDevices = mock.Mock(
            return_value={
                "general_camera_manage": {
                    "paired_general_device_list": [
                        {
                            "alias": "Not On Hub",
                            "device_id": "disabled-id",
                            "hub_storage_enabled": False,
                            "mac": "111122223333",
                        },
                        {
                            "alias": "New Camera",
                            "device_id": "new-camera-id",
                            "hub_storage_enabled": True,
                            "mac": "AABBCCDDEEFF",
                        },
                    ]
                }
            }
        )

        assert tapo._findHubStorageChild("disabled-id") is None
        assert tapo._findHubStorageChild("new-camera-id")["alias"] == "New Camera"
        assert tapo._findHubStorageChild("aa:bb:cc:dd:ee:ff")["device_id"] == "new-camera-id"
