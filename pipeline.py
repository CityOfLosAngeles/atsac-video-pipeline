"""
Pipeline for tranferring ATSAC videos to the cloud and processing them.
"""
import os
import shutil
import sys

import fsspec

import streamz
import tornado


def check_manifest(x: str, manifest: str) -> bool:
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
    of = fsspec.open(manifest, "r")
    if not of.fs.exists(manifest):
        return True
    # If the file exists, check if the file exists in it.
    with of as f:
        content = set(f.read().split("\n"))
        return x not in content


def add_to_manifest(x: str, manifest: str) -> None:
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
    of = fsspec.open(manifest, "r")
    if not of.fs.exists(manifest):
        content = set([x])
    else:
        with fsspec.open(manifest, "r") as infile:
            content = set(infile.read().split("\n"))
            content.add(x)
    # Write to the manifest.
    with fsspec.open(manifest, "w") as outfile:
        outfile.write("\n".join(sorted(list(content))))


def process(path: str, manifest: str) -> None:
    """
    Process a file. Once it is done, marks the file in the manifest and deletes it.

    Parameters
    ==========

    path: str
        The path to the file to process.

    manifest: str
        The path to a manifest to mark the file has having been processed.
    """
    print(f"Processing {path}")
    add_to_manifest(path, manifest)
    return


def upload(src: str, dst: str, manifest: str) -> None:
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
    print(f"Uploading {src}")
    shutil.copyfile(src, os.path.join(dst, os.path.basename(src)))
    add_to_manifest(src, manifest)
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
            filenames = set(fs.glob(self.glob))
            new = filenames - self.seen
            for fn in sorted(new):
                self.seen.add(fn)
                yield self._emit(fn)
            yield tornado.gen.sleep(self.poll_interval)  # TODO: remove poll if delayed
            if self.stopped:
                break


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Incorrect arguments")
        exit()

    if sys.argv[1] == "upload":
        fs = fsspec.filesystem("file")
        paths = PathsSource(fs, os.path.join(os.getcwd(), "atsac/*.csv"))
        paths.filter(lambda x: check_manifest(x, "uploaded.txt")).sink(
            lambda x: upload(x, os.path.join(os.getcwd(), "cloud"), "uploaded.txt")
        )
        paths.start()
    elif sys.argv[1] == "process":
        fs = fsspec.filesystem("file")
        paths = PathsSource(fs, os.path.join(os.getcwd(), "cloud/*.csv"))
        paths.filter(lambda x: check_manifest(x, "processed.txt")).sink(
            lambda x: process(x, "processed.txt")
        )
        paths.start()

    tornado.ioloop.IOLoop().start()
