"""
Pipeline for tranferring ATSAC videos to the cloud and processing them.
"""
import argparse
import os
import subprocess

import fsspec
import streamz
import tornado


def check_manifest(of: fsspec.core.OpenFile, manifest: str) -> bool:
    """
    Check to see if a given string exists in a manifest file.

    Parameters
    ==========

    x: str
        The string to check.
    manifest: str
        The path to a manifest file.

    Returns
    =======

    True if the file is *not* in the manifest, False if it is.
    """
    # Check if the file actually exists. If not, return true.
    mf = fsspec.open(manifest, "r")
    if not mf.fs.exists(manifest):
        return True
    # If the file exists, check if the file exists in it.
    with mf as f:
        content = set(f.read().split("\n"))
        return of.path not in content


def add_to_manifest(of: fsspec.core.OpenFile, manifest: str) -> None:
    """
    Add a string to a manifest file.

    Parameters
    ==========

    x: str
        The string to add.
    manifest: str
        The path to a manifest file.
    """
    # If the manifest exists, load it. Otherwise, create a new set seeded
    # with the "x" string.
    mf = fsspec.open(manifest, "r")
    if not mf.fs.exists(manifest):
        content = set([of.path])
    else:
        with fsspec.open(manifest, "r") as infile:
            content = set(infile.read().split("\n"))
            content.add(of.path)
    # Write to the manifest.
    with fsspec.open(manifest, "w") as outfile:
        outfile.write("\n".join(sorted(list(content))))


def process(of: fsspec.core.OpenFile, manifest: str) -> None:
    """
    Process a file. Once it is done, marks the file in the manifest and deletes it.

    Parameters
    ==========

    path: str
        The path to the file to process.

    manifest: str
        The path to a manifest to mark the file has having been processed.
    """
    print(f"Processing {of.path}...", end="", flush=True)

    # Download the data locally
    local = os.path.join("/tmp", os.path.basename(of.path))
    of.fs.get(of.path, local)

    # Run the walk/bike algorithm
    process = subprocess.run(
        ["automated-walk-bike-counter", "--config=config.ini", f"--file_name={local}"],
        capture_output=True,
        check=False,
    )

    # Mark as processed
    add_to_manifest(of, manifest)

    # Upload log and CSV to remote filesystem
    csvfile = os.path.splitext(local)[0] + ".csv"
    with of.fs.open(os.path.splitext(of.path)[0] + ".log", "wb") as out:
        out.write(process.stdout)
    of.fs.upload(csvfile, os.path.splitext(of.path)[0] + ".csv")

    # Remove data
    os.remove(local)
    os.remove(csvfile)
    of.fs.rm(of.path)
    print(f"done")
    return


def upload(of: fsspec.core.OpenFile, dst: str, manifest: str) -> None:
    """
    Upload a file to a destination path so it can be picked up for processing.
    Once it is done, marks it in the manifest.

    Parameters
    ==========

    src: str
        The source file to upload.

    dst: str
        The destination directory/bucket to which to upload the file.

    manifest: str
        A path to a manifest to flag the file as having been uploaded.
    """
    print(f"Uploading {of.path}...", end="", flush=True)
    # Download the data locally
    name = os.path.basename(of.path)
    local = os.path.join("/tmp", name)
    of.fs.get(of.path, local)

    target = fsspec.open(dst, "wb")
    target.fs.upload(local, os.path.join(dst, name))
    add_to_manifest(of, manifest)
    print(f"done")
    return


class PathsSource(streamz.Source):
    """
    A streamz.Source that emits paths from an fsspec AbstractFileSystem.
    Generalized from streamz.Source.filenames.
    """

    def __init__(
        self,
        fs: fsspec.AbstractFileSystem,
        glob: str = "*",
        poll_interval: float = 5.0,
        start=False,
        **kwargs,
    ):
        """
        Construct the paths source.

        Parameters
        ==========

        fs: fsspec.AbstractFileSystem
            The filesystem to use as a source.

        glob: str
            A glob pattern to list files.

        poll_interval: float
            How often to poll the filesystem. Longer than that in Source.filenames
            because it may be a remote source.

        start: bool
            Whether to start polling on construction.
        """
        self.fs = fs
        self.glob = glob
        self.seen = set()
        self.poll_interval = poll_interval
        self.stopped = True
        super(PathsSource, self).__init__(ensure_io_loop=True)
        if start:
            self.start()

    def start(self):
        """
        Start the stream.
        """
        self.stopped = False
        self.loop.add_callback(self.do_poll)

    @tornado.gen.coroutine
    def do_poll(self):
        """
        Poll the abstract file system.
        """
        while True:
            filenames = set(self.fs.glob(self.glob, refresh=True))
            new = filenames - self.seen
            for fn in sorted(new):
                self.seen.add(fn)
                yield self._emit(self.fs.open(fn, "rb"))
            yield tornado.gen.sleep(self.poll_interval)  # TODO: remove poll if delayed
            if self.stopped:
                break


def do_upload(args):
    """
    Run the uploading pipeline.
    """
    # TODO: don't assume only local here
    fs = fsspec.filesystem("file", use_listings_cache=False)
    manifest = os.path.join(args.dest, "uploaded.txt")
    paths = PathsSource(fs, args.glob)
    paths.filter(lambda x: check_manifest(x, manifest)).sink(
        lambda x: upload(x, args.dest, manifest)
    )
    paths.start()

    tornado.ioloop.IOLoop().start()


def do_process(args):
    """
    Run the processing pipeline.
    """
    # TODO: don't assume only s3 here.
    fs = fsspec.filesystem("s3")
    paths = PathsSource(fs, args.src)
    manifest = os.path.join(os.path.dirname(args.src), "processed.txt")
    paths.filter(lambda x: check_manifest(x, manifest)).sink(
        lambda x: process(x, manifest)
    )
    paths.start()

    tornado.ioloop.IOLoop().start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    upload_parser = subparsers.add_parser("upload")
    upload_parser.add_argument(
        "glob", type=str, help="A glob pattern matching videos to upload",
    )
    upload_parser.add_argument(
        "dest", type=str, help="A destination to upload the videos to",
    )
    upload_parser.set_defaults(func=do_upload)

    process_parser = subparsers.add_parser("process")
    process_parser.add_argument(
        "src", type=str, help="The source location for videos to process",
    )
    process_parser.set_defaults(func=do_process)

    args = parser.parse_args()
    args.func(args)
