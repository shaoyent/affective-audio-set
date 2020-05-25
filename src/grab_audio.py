import os

import glob
import logging
import argparse

from functools import partial
from tqdm import tqdm

import youtube_dl

from multiprocessing import Pool

def grab_audio( vid_id, save_path='./', ydl_opts={}) :

    if glob.glob( os.path.join(save_path, "{}*.wav".format(vid_id))) and ydl_opts.get('nooverwrites', True) :
        logging.warning("Audio with ID {} exists in path. Enable overwrite to re-download".format(vid_id))

    else :
        try :
            with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                ydl.download(['https://www.youtube.com/watch?v={}'.format(vid_id)])
        except Exception as e :
            logging.error(e)
            return False

    return True


def main():

    parser = argparse.ArgumentParser("Grab audio YouTube videos")

    parser.add_argument("--csv", type=str, default=None, required=True, help='Comma-separated file of video IDs')
    parser.add_argument("--save_path", type=str, default='./downloaded_files', help='Path to save downloaded files')
    parser.add_argument("--proxy", type=str, default=None, help='Proxy to use for youtube-dl')
    parser.add_argument("--nj", type=int, default=4, help='Number of parallel jobs')
    parser.add_argument("--overwrite", default=False, action="store_true", help='Enable overwriting files')

    args = parser.parse_args()

    logging.basicConfig(filename='grab_audio.log', filemode='a', level=logging.DEBUG)
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
        'nooverwrites': not args.overwrite,
    }
    logger.debug(ydl_opts)

    if args.proxy is not None :
        ydl_opts.update( {'proxy': args.proxy} )

    pool_fn = partial(grab_audio,
                    save_path=args.save_path,
                    ydl_opts=ydl_opts
                )

    logger.info("Grabbing audio from {}".format(args.csv))
    with Pool(args.nj) as pool :

        ret = list(tqdm( pool.imap_unordered(pool_fn, vid_ids), total=len(vid_ids)))

if __name__ == "__main__" :
    main()


