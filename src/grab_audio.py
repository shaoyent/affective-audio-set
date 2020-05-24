import os

import logging
import argparse

from functools import partial
from tqdm import tqdm

import youtube_dl

from multiprocessing import Pool

def grab_audio( vid_id, save_path='./', ydl_opts={}) :
    
    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        ydl.download(['https://www.youtube.com/watch?v={}'.format(vid_id)])


def main():

    parser = argparse.ArgumentParser("Grab audio YouTube videos")

    parser.add_argument("--csv", type=str, default=None, required=True, help='Comma-separated file of video IDs')
    parser.add_argument("--save_path", type=str, default='./downloaded_files', help='Path to save downloaded files')
    parser.add_argument("--nj", type=int, default=4, help='Number of parallerl jobs')

    args = parser.parse_args()

    logging.basicConfig(filename='grab_audio.log', filemode='w', level=logging.INFO)
    logger = logging.getLogger()
    logger.info(args)

    os.makedirs(args.save_path, exist_ok=True)

    vid_ids = []
    with open(args.csv, 'r') as fh :
        for line in fh :
            vid = line.strip().split(',')[0]

            if vid :
                vid_ids.append(vid)

    ydl_opts = {
        'format': 'bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'wav',
        }],
        'logger': logger,
        'restrictfilenames': True,
        'outtmpl': '{}/%(id)s-%(title)s.%(ext)s'.format(args.save_path), 
        'writeinfojson': True,
        'writedescription': True,
        'writesubtitles': True,
    }

    pool_fn = partial(grab_audio,
                    save_path=args.save_path,
                    ydl_opts=ydl_opts
                )

    with Pool(args.nj) as pool :

        ret = tqdm( pool.imap_unordered(pool_fn, vid_ids), total=len(vid_ids)) 

        for r in ret :
            continue


if __name__ == "__main__" :
    main()


