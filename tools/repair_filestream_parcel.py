"""Create a new parcel containing only the runtime-matched bundled FileStream.

Never edits the input or extracts files. Unknown/custom FileStream locations
require manual review instead of being silently removed.
"""
import argparse
import copy
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import tarfile
import tempfile


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def repair(source, destination):
    source, destination = Path(source), Path(destination)
    if destination.exists() or source.resolve() == destination.resolve():
        raise ValueError("Output must be a new file; original parcels are never overwritten")
    jars, names, links = {}, set(), []
    canonical = None
    with tarfile.open(source, "r|gz") as archive:
        for member in archive:
            name = member.name.removeprefix("./").rstrip("/")
            if name in names:
                raise ValueError("Duplicate archive entry: " + name)
            names.add(name)
            if member.issym() or member.islnk():
                links.append(member)
            if not (PurePosixPath(name).name.startswith("connect-file-") and name.endswith(".jar")):
                continue
            parts = name.split("/")
            relative = "/".join(parts[1:-1])
            if relative not in {"libs", "share/filestream-connectors", "connect-plugins/filestream-connectors", "connect-plugins/tantor-filestream"}:
                raise ValueError("Custom FileStream location requires review: " + name)
            if not member.isfile() or member.size > 32 * 1024 * 1024:
                raise ValueError("Expected a regular bundled FileStream JAR: " + name)
            data = archive.extractfile(member).read()
            jars[name] = (copy.copy(member), data)
            if relative == "libs":
                if canonical is not None:
                    raise ValueError("Multiple runtime FileStream JARs; select the runtime explicitly")
                canonical = name
    if canonical is None:
        raise ValueError("Runtime libs/connect-file-*.jar is required")
    root = canonical.split("/")[0]
    header, data = jars[canonical]
    version = PurePosixPath(canonical).name.removeprefix("connect-file-").removesuffix(".jar")
    if root + "/libs/connect-runtime-" + version + ".jar" not in names:
        raise ValueError("FileStream does not match the packaged Connect runtime")
    removed = set(jars) - {canonical}
    for link in links:
        if any(PurePosixPath(name).name in link.linkname for name in removed):
            raise ValueError("A parcel link references a removed copy: " + link.name)
    fd, temporary = tempfile.mkstemp(prefix=".filestream-parcel-", dir=destination.parent)
    os.close(fd)
    try:
        with tarfile.open(source, "r|gz") as old, tarfile.open(temporary, "w:gz", compresslevel=6) as new:
            for member in old:
                name = member.name.removeprefix("./").rstrip("/")
                if name in removed:
                    continue
                new.addfile(member, old.extractfile(member) if member.isfile() else None)
            target = copy.copy(header)
            target.name = root + "/connect-plugins/tantor-filestream/" + PurePosixPath(canonical).name
            target.mode = 0o644
            new.addfile(target, io.BytesIO(data))
        # Exclusive creation, including when another process creates output meanwhile.
        os.link(temporary, destination)
    finally:
        os.unlink(temporary)
    return {"source": str(source), "sourceSha256": digest(source), "output": str(destination),
            "sha256": digest(destination), "runtimeFileStream": canonical,
            "fileStreamSha256": hashlib.sha256(data).hexdigest(), "removedCopies": sorted(removed)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source")
    parser.add_argument("destination")
    args = parser.parse_args()
    print(json.dumps(repair(args.source, args.destination), indent=2))
