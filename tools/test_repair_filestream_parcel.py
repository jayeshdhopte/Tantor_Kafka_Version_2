import io
from pathlib import Path
import tarfile
import tempfile
import unittest
from repair_filestream_parcel import digest, repair


class ParcelRepairTest(unittest.TestCase):
    def test_corrects_bundle_preserves_custom_plugins_and_original(self):
        with tempfile.TemporaryDirectory() as directory:
            source, output = Path(directory)/"original.tgz", Path(directory)/"corrected.tgz"
            entries = {"kafka/libs/connect-runtime-4.3.0.jar": b"runtime",
                       "kafka/libs/connect-file-4.3.0.jar": b"canonical",
                       "kafka/share/filestream-connectors/connect-file-8.1.1-ccs.jar": b"vendor",
                       "kafka/connect-plugins/filestream-connectors/connect-file-8.1.1-ccs.jar": b"vendor",
                       "kafka/connect-plugins/custom/custom.jar": b"preserve"}
            with tarfile.open(source, "w:gz") as archive:
                for name, body in entries.items():
                    header = tarfile.TarInfo(name); header.size = len(body)
                    archive.addfile(header, io.BytesIO(body))
            original_hash = digest(source)
            result = repair(source, output)
            self.assertEqual(digest(source), original_hash)
            self.assertEqual(len(result["removedCopies"]), 2)
            with tarfile.open(output) as archive:
                self.assertEqual(archive.extractfile("kafka/connect-plugins/custom/custom.jar").read(), b"preserve")
                self.assertEqual(archive.extractfile("kafka/connect-plugins/tantor-filestream/connect-file-4.3.0.jar").read(), b"canonical")
                self.assertFalse(any("connect-file-8.1.1" in name for name in archive.getnames()))
            with self.assertRaises(ValueError): repair(source, source)
            with self.assertRaises(ValueError): repair(source, output)

    def test_unknown_custom_filestream_location_is_not_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            source, output = Path(directory)/"original.tgz", Path(directory)/"corrected.tgz"
            with tarfile.open(source, "w:gz") as archive:
                body=b"custom"
                header=tarfile.TarInfo("kafka/connect-plugins/customer/connect-file-4.0.jar"); header.size=len(body)
                archive.addfile(header, io.BytesIO(body))
            with self.assertRaisesRegex(ValueError,"Custom FileStream location"): repair(source,output)
            self.assertFalse(output.exists())


if __name__ == "__main__": unittest.main()
