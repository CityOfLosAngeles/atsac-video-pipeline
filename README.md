# ATSAC Video Pipeline

This repository holds support scripts for running the
[automated-walk-bike-counter](https://github.com/CityOfLosAngeles/automated-walk-bike-counter)
on video files produced by ATSAC traffic cameras.

## Background

Currently ATSAC has 600 analog cameras streaming video continuously
to their control center. There are up to 12 encoders that can produce
digital video from these feeds and place those videos on a single
shared drive. However, this shared drive is not (currently) an appropriate
place to run the walk/bike algorithm, so we want to copy the files
to a place that can run the algorithm, run it, and delete the videos.

The `pipeline.py` script is intended to be run in *two* places.
First, an internet-connected location with the shared ATSAC drive mounted.
Second, a GPU compute instance that will run the data.
These places do not need to know anything about each other,
and all communication is mediated through putting files in an s3 bucket.

## Installation

The dependencies in `requirements.txt` must be installed into your environment.
On the processing side, this should be the same environment as the walk/bike algorithm.

## Usage

On the upload side, run
```bash
python pipeline.py upload /path/to/files
```
where `path/to/files` is a glob path that matches the files on the shared drive.

On the processing side, run
```bash
python pipeline.py process
```
