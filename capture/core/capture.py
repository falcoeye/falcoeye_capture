import logging
import re
import subprocess as sp
import cv2
import json
import numpy as np
import requests
import streamlink
from datetime import datetime
import tempfile
import os
import ffmpeg
from PIL import Image
import io
from .utils import get_service,put, random_string,tempdir,internal_err_resp,message,mkdir

class StreamingServerSource:
    @staticmethod
    def create_stream_pipe(url, resolution):
        if url is None:
            return None

        try:
            streams = streamlink.streams(url)
        except streamlink.exceptions.NoPluginError:
            logging.warning(f"Warning: NO STREAM AVAILABLE in {url}")
            return None
        chosen_res = None
        logging.info(f"streams are found")
        for r in resolution:
            if r in streams and hasattr(streams[r], "url"):
                stream_url = streams[r].url
                chosen_res = r
                break
        logging.info(f"chosen stream {stream_url} {chosen_res}")

        logging.info(f"Proping stream {stream_url}")
        p = StreamingServerSource.probe_stream(stream_url)
        
        
        ffmpeg = "/usr/local/bin/ffmpeg"
        if not os.path.exists(ffmpeg):
            ffmpeg = "/usr/bin/ffmpeg"
        pipe = sp.Popen(
            [
                ffmpeg,
                "-i",
                stream_url,
                "-loglevel",
                "quiet",  # no text output
                "-an",  # disable audio
                "-f",
                "image2pipe",
                "-pix_fmt",
                "bgr24",
                "-vcodec",
                "rawvideo",
                "-",
            ],
            stdin=sp.PIPE,
            stdout=sp.PIPE,
        )
        return pipe, p

    @staticmethod
    def probe_stream(stream_url):
        p = ffmpeg.probe(stream_url, select_streams='v')
        return p["streams"][0]

    @staticmethod
    def read(streamer, width, height):
        raw_image = streamer.stdout.read(height * width * 3)
        frame = np.fromstring(raw_image, dtype="uint8").reshape((height, width, 3))
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        return frame

    @staticmethod
    def record_video(streamer, width, height, length, filename):
        # TODO: handle errors
        count_frame = 0
        lengthFrames = length * 30  # Assuming 30 frames per second
        logging.info(f"Starting recording. Will grab {lengthFrames} frames")
        frames = np.zeros((lengthFrames, height, width, 3), dtype=np.uint8)
        while count_frame < lengthFrames:
            raw_image = streamer.stdout.read(height * width * 3)
            logging.info(f"Frame {count_frame+1}/{lengthFrames}")
            frames[count_frame] = (
                np.fromstring(raw_image, dtype="uint8")
                .reshape((height, width, 3))
                .astype(np.uint8)
            )
            count_frame += 1
        logging.info(f"Starting writing video. Will write it in {filename}")
        tempfile =  f'{tempdir()}/{datetime.now().strftime("%m_%d_%Y")}_{random_string()}.mp4'
        logging.info(f"Creating temp file first {tempfile}")
        fourcc = cv2.VideoWriter_fourcc(*'H264')
        writer = cv2.VideoWriter(
            tempfile,
            fourcc=fourcc,
            fps=30,
            frameSize=(width, height),
            isColor=True,
        )
        logging.info(f"Writing to temp file  {tempfile}")
        for frame in frames:
            writer.write(frame)
        writer.release()
        # from bgr to rgb
        thumbnail_frame = frames[0].copy()[:,:,::-1]
        # freeing memory
        frames = None
        return True,tempfile,thumbnail_frame

class AngelCamSource(StreamingServerSource):
    resolutions = {"best": {"width": 1920, "height": 1080}}

    @staticmethod
    def open(url):
        c = requests.get(url).content.decode("utf-8")
        m3u8 = re.findall(r"\'https://.*angelcam.*token=.*\'", c)[0].strip("'")
        streamer, chosen_res = StreamingServerSource.create_stream_pipe(m3u8, ["best"])
        res = AngelCamSource.resolutions[chosen_res]
        width, height = res["width"], res["height"]
        return streamer, width, height

    @staticmethod
    def capture_image(url):
        streamer, width, height = AngelCamSource.open(url)
        frame = StreamingServerSource.read(streamer, width, height)
        streamer.kill()
        return frame

    @staticmethod
    def record_video(url, length, filename):
        streamer, width, height = AngelCamSource.open(url)
        succeeded,tmp_path,thumbnail_frame = StreamingServerSource.record_video(
            streamer, width, height, length, filename
        )
        streamer.kill()
        return succeeded,tmp_path,thumbnail_frame

class M3U8Source(StreamingServerSource):
    resolutions = {"best": {"width": 320, "height": 180}}
    @staticmethod
    def open(url):
        m3u8 = url
        streamer, probe = StreamingServerSource.create_stream_pipe(m3u8, ["best"])
        width, height = probe["width"],probe["height"]
        logging.info(f"Stream opened with resolution: {width}X{height}")
        return streamer, width, height

    @staticmethod
    def capture_image(url):
        streamer, width, height = M3U8Source.open(url)
        frame = StreamingServerSource.read(streamer, width, height)
        streamer.kill()
        return frame

    @staticmethod
    def record_video(url, length, filename):
        streamer, width, height = M3U8Source.open(url)
        succeeded,tmp_path,thumbnail_frame = StreamingServerSource.record_video(
            streamer, width, height, length, filename
        )
        streamer.kill()
        return succeeded,tmp_path,thumbnail_frame

class YoutubeSource(StreamingServerSource):
    resolutions = {
        "240p": {"width": 426, "height": 240},
        "360p": {"width": 640, "height": 360},
        "480p": {"width": 854, "height": 480},
        "720p": {"width": 1280, "height": 720},
        "1080p": {"width": 1920, "height": 1080},
    }

    @staticmethod
    def open(url):
        logging.info(f"Opening streamer {url}")
        streamer, probe = StreamingServerSource.create_stream_pipe(
            url, ["1080p", "720p", "480p", "360p", "240p"]
        )
        #logging.info(f"Chosen resolution {chosen_res}")
        # res = YoutubeSource.resolutions[chosen_res]
        # width, height = res["width"], res["height"]
        width, height = probe["width"],probe["height"]
        logging.info(f"Stream opened with resolution: {width}X{height}")
        return streamer, width, height

    @staticmethod
    def capture_image(url):
        logging.info(f"Capturing image from youtube source {url}")
        streamer, width, height = YoutubeSource.open(url)
        logging.info(f"Streamer opened width: {width} height: {height}")
        frame = StreamingServerSource.read(streamer, width, height)
        logging.info("Capturing finished. Killing streamer")
        streamer.kill()
        return frame

    @staticmethod
    def record_video(url, length, filename):
        streamer, width, height = YoutubeSource.open(url)
        logging.info(f"Streamer opened with width={width} height={height}")
        succeeded, tmp_path, thumbnail_frame = StreamingServerSource.record_video(
            streamer, width, height, length, filename
        )
        logging.info("Recording finished. Killing streamer")
        streamer.kill()
        return succeeded,tmp_path,thumbnail_frame

class RTSPSource:
    @staticmethod
    def open(ipv4, port, username, password):
        url = "rtsp://"
        if username != "":
            url += f"{username}:{password}@"
        url += f"{ipv4}:{port}/Streaming/Channels/1"
        streamer = cv2.VideoCapture(url)
        return streamer

    @staticmethod
    def capture_image(ipv4, port, username, password):
        streamer = RTSPSource.open(ipv4, port, username, password)
        ret, frame = streamer.read()
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        streamer.release()
        return frame

    @staticmethod
    def record_video(ipv4, port, username, password, length, filename):
        streamer = RTSPSource.open(ipv4, port, username, password)
        width = int(streamer.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(streamer.get(cv2.CAP_PROP_FRAME_HEIGHT))

        count_frame = 0
        lengthFrames = length * 30  # Assuming 30 frames per second
        frames = np.zeros((lengthFrames, height, width, 3),dtype=np.uint8)

        while count_frame < lengthFrames:
            ret, frame = streamer.read()
            if not ret:
                break
            frames[count_frame] = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB).astype(np.uint8)
            count_frame += 1

        streamer.release()
        logging.info(f"Starting writing video. Will write it in {filename}")
        tempfile =  f'{tempdir()}/{datetime.now().strftime("%m_%d_%Y")}_{random_string()}.mp4'
        logging.info(f"Creating temp file first {tempfile}")
        fourcc = cv2.VideoWriter_fourcc(*'H264')
        writer = cv2.VideoWriter(
            tempfile,
            fourcc=fourcc,
            fps=30,
            frameSize=(width, height),
            isColor=True,
        )
        logging.info(f"Writing to temp file  {tempfile}")
        for frame in frames:
            writer.write(frame)
        writer.release()
        # from bgr to rgb
        thumbnail_frame = frames[0].copy()[:,:,::-1]
        # freeing memory
        frames = None
        return True, tempfile,thumbnail_frame

def capture_image_from_streaming_server(url):
    if "youtube" in url:
        logging.info(f"Recording from youtube source {url}")
        return YoutubeSource.capture_image(url)
    elif "angelcam" in url:
        return AngelCamSource.capture_image(url)
    elif url.endswith(".m3u8"):
        return M3U8Source.capture_image(url)

def capture_image_from_rtsp(host, port, username, password):
    return RTSPSource.capture_image(host, port, username, password)

def capture_image(camera):
    if "url" in camera:
        logging.info(f"Recording from streaming server {camera['url']}")
        return capture_image_from_streaming_server(camera["url"])
    else:
        return capture_image_from_rtsp(
            camera["host"], camera["port"], camera["username"], camera["password"]
        )

def record_video_from_streaming_server(url, length, outputpath):
    if "youtube" in url:
        logging.info("Recording from youtube server")
        return YoutubeSource.record_video(url, length, outputpath)
    elif "angelcam" in url:
        return AngelCamSource.record_video(url, length, outputpath)
    elif url.endswith(".m3u8"):
        return M3U8Source.record_video(url, length, outputpath)

def record_video_from_rtsp(host, port, username, password, length, outputpath):
    return RTSPSource.record_video(host, port, username, password, length, outputpath)

def record_video(camera, length, outputpath):
    if "url" in camera:
        logging.info(f"Recording from streaming server {camera['url']}")
        return record_video_from_streaming_server(camera["url"], length, outputpath)
    else:
        return record_video_from_rtsp(
            camera["host"],
            camera["port"],
            camera["username"],
            camera["password"],
            length,
            outputpath,
        )

def generate_thumbnail(url):
    streamer = cv2.VideoCapture(url)
    ret, frame = streamer.read()
    frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    streamer.release()
    return frame

def post_back(registry_key,capture_status):
    try:
        backend_server = get_service("falcoeye-backend")
        postback_url = f"{backend_server}/api/capture/{registry_key}"
        resp = message(True, "capture completed")
        resp["capture_status"] = capture_status
        logging.info(f"Posting new status {capture_status} to backend {postback_url}")
        rv = requests.put(
            postback_url,
            data=json.dumps(resp),
            headers={"Content-type": "application/json","X-API-KEY":os.environ.get("JWT_KEY")},
        )
        if rv.headers["content-type"].strip().startswith("application/json"):
            logging.info(f"Response received {rv.json()}")
        else:
            logging.warning(f"Request might have failed. No json response received")
    except requests.exceptions.ConnectionError:
        logging.error(
            f"Warning: failed to inform backend server ({backend_server}) for change in the status "
            f"of: {registry_key} due to ConnectionError"
        )
    except requests.exceptions.Timeout:
        logging.error(
            f"Warning: failed to inform backend server ({backend_server}) for change in the status "
            f"of: {registry_key} due to Timeout"
        )
    except requests.exceptions.HTTPError:
        logging.error(
            f"Warning: failed to inform backend server ({backend_server}) for change in the status "
            f"of: {registry_key} due to HTTPError"
        )

class CaptureRunner:
    @staticmethod
    def capture(registry_key, camera, output_path, **args):
        logging.info(f"Capturing image for {registry_key} from {camera} and store it in {output_path}")
        image = capture_image(camera)
        #image = np.ones((100,100,3),dtype=np.uint8)
        if image is not None:
            fdir = os.path.dirname(output_path)
            logging.info(f"Making directory {fdir}")
            mkdir(fdir)
            img = Image.fromarray(image)
            with open(os.path.relpath(output_path), "wb") as f:
                byteImgIO = io.BytesIO()
                img.save(byteImgIO, "JPEG")
                byteImgIO.seek(0)
                byteImg = byteImgIO.read()
                f.write(byteImg)
            
            thumbnail_path = f"{os.path.splitext(output_path)[0]}_260.jpg"
            logging.info(f"Creating thumbnail image {thumbnail_path}")
            with open(os.path.relpath(thumbnail_path), "wb") as f:
                byteImgIO = io.BytesIO()
                img.thumbnail((260,260))
                logging.info(f"thumbnail size {img.size}")
                img.save(byteImgIO, "JPEG")
                byteImgIO.seek(0)
                byteImg = byteImgIO.read()
                f.write(byteImg)
            capture_status = "SUCCEEDED"
        else:
            capture_status = "FAILED"

        post_back(registry_key,capture_status)
    
    @staticmethod
    def record(registry_key, camera, output_path, length=60, **args):
        # in case string
        length = int(length)

        logging.info(f"Recording video with camera {camera} for {length} seconds")
        recorded, tmp_path,thumbnail_frame = record_video(camera, length, output_path)
        logging.info(f"Video recorded? {recorded}")
        if recorded:
            fdir = os.path.dirname(output_path)
            logging.info(f"Making directory {fdir}")
            mkdir(fdir)
            logging.info(f"Moving recording from {tmp_path} to {output_path}")
            put(tmp_path,output_path)
            thumbnail_path = f"{os.path.splitext(output_path)[0]}_260.jpg"
            logging.info(f"Creating thumbnail image {thumbnail_path}")
            with open(os.path.relpath(thumbnail_path), "wb") as f:
                byteImgIO = io.BytesIO()
                img = Image.fromarray(thumbnail_frame)
                img.thumbnail((260,260))
                img.save(byteImgIO, "JPEG")
                byteImgIO.seek(0)
                byteImg = byteImgIO.read()
                f.write(byteImg)

            logging.info(f"Removing {tmp_path}")
            #os.remove(tmp_path)
            capture_status = "SUCCEEDED"
        else:
            capture_status = "FAILED"
        
        post_back(registry_key,capture_status)
        
    @staticmethod
    def generate_thumbnail(video_file, output_path, **args):
        thumbnail = generate_thumbnail(video_file)
        logging.info(f"Creating thumbnail image {output_path}")
        img = Image.fromarray(thumbnail)
        img.thumbnail((260,260))
        with open(os.path.relpath(output_path), "wb") as f:
            byteImgIO = io.BytesIO()
            img.save(byteImgIO, "JPEG")
            byteImgIO.seek(0)
            byteImg = byteImgIO.read()
            f.write(byteImg)
        logging.info("Thumbnail created")
        resp = message(True, "thumbnail generated")
        return resp, 200 

    @staticmethod
    def run_from_dict(capture_dict):
        try:
            logging.info(capture_dict)
            if capture_dict["type"] == "image":
                return CaptureRunner.capture(**capture_dict)
            elif capture_dict["type"] == "video":
                return  CaptureRunner.record(**capture_dict)
            elif capture_dict["type"] == "thumbnail":
                return CaptureRunner.generate_thumbnail(**capture_dict)
        except Exception as error:
            logging.error(error)
            return internal_err_resp()
    
