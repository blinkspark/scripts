#!/usr/bin/python3
import os
import qiniu
import argparse
import requests
import time
# a cli for qiniu Kodo


def upload_file(filename, access_token, secret_token, bucket_name, prefix="", delimiter="/"):
    # upload file to qiniu bucket
    # upload file to qiniu bucket
    q = qiniu.Auth(access_token, secret_token)
    key = filename
    token = q.upload_token(bucket_name, key)
    ret, info = qiniu.put_file(token, key, filename)
    if ret is not None:
        print("upload success")
    else:
        print("upload failed")
        print(info)


def print_items(items):
    for item in items:
        key = item["key"]
        hash = item["hash"]
        size = item["fsize"]
        print(f"{key}\t{hash}\t{size}")


def list_objects(bucket_name, prefix, delimiter, access_token, secret_token):
    # list objects of bucket
    q = qiniu.Auth(access_token, secret_token)
    bucket = qiniu.BucketManager(q)
    ret, eof, info = bucket.list(
        bucket_name, prefix=prefix, marker=None, limit=None, delimiter=delimiter)
    if ret is not None:
        # print("list success")
        print_items(ret.get("items"))
        while eof == False:
            ret, eof, info = bucket.list(
                bucket_name, prefix=prefix, marker=ret.get("marker"), limit=None, delimiter=delimiter)
            if ret is not None:
                print_items(ret.get("items"))
            else:
                print("list failed")
                print(info)  # list objects of qiniu bucket
                break
    else:
        print("list failed")
        print(info)  # list objects of qiniu bucket


def get_object(host, key, out_path, access_token, secret_token):
    # get object from qiniu bucket
    q = qiniu.Auth(access_token, secret_token)
    url = f"{host}/{key}"
    priv_url = q.private_download_url(url)
    res = requests.get(priv_url)
    if res.status_code == 200:
        with open(out_path, "wb") as f:
            f.write(res.content)
    else:
        print(res.status_code)


def archive_and_upload(inpath, outpath, bucket_name,
                       access_token, secret_token, prefix="", delimiter="/"):
    ltime = time.localtime()
    fmt_time = time.strftime("%Y%m%d", ltime)
    fmt_filename = f"{outpath}-{fmt_time}.tar.gz"
    print(fmt_filename)
    res = os.system(f"tar -zcf {fmt_filename} {inpath}")
    if res == 0 or res == 256:
        upload_file(fmt_filename, access_token,
                    secret_token, bucket_name, prefix, delimiter)
        os.system(f"rm {fmt_filename}")
    else:
        print(f"tar error {res}")


def main():
    # get access_token, secret_token, bucket_name from env
    access_token = os.getenv("QN_ACCESS_TOKEN")
    secret_token = os.getenv("QN_SECRET_TOKEN")
    # bucket_name = os.getenv("QN_BUCKET_NAME")

    # parse args
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers(
        dest="sub_cmd", help="sub-command help", required=True)
    # upload subcommand
    upload = subs.add_parser("upload")
    upload.add_argument("-k", "--key")
    upload.add_argument(
        "-i", "--inpath", help="(reqired) input is either a file or a directory", required=True)
    upload.add_argument(
        "-b", "--bucket", help="(reqired) bucket name of qiniu", required=True)
    # get subcommand
    get = subs.add_parser("get")
    get.add_argument("-k", "--key", help="(reqired)", required=True)
    get.add_argument("-u", "--url", help="(reqired) ", required=True)
    get.add_argument(
        "-o", "--outpath", help="(reqired) output path", required=True)
    # ls : list objects of bucket
    ls = subs.add_parser("ls")
    ls.add_argument("-b", "--bucket", help="(reqired)", required=True)
    ls.add_argument("-p", "--prefix", help="prefix of key", default="")
    ls.add_argument("-d", "--delimiter",
                    help="delimiter of key defalaut=/", default="/")
    # archive subcommand
    archive = subs.add_parser("archive")
    archive.add_argument("-i", "--inpath", help="(reqired)", required=True)
    archive.add_argument("-o", "--outpath", help="(reqired)", required=True)
    archive.add_argument("-b", "--bucket", help="(reqired)", required=True)
    archive.add_argument("-p", "--prefix", help="prefix of key", default="")
    archive.add_argument("-d", "--delimiter",
                         help="delimiter of key defalaut=/", default="/")

    args = parser.parse_args()
    print(f"debug:{args}")

    if args.sub_cmd == "upload":
        upload_file(args.inpath, access_token, secret_token, args.bucket)
    elif args.sub_cmd == "ls":
        list_objects(args.bucket, args.prefix, args.delimiter,
                     access_token, secret_token)
    elif args.sub_cmd == "get":
        get_object(args.url, args.key, args.outpath,
                   access_token, secret_token)
    elif args.sub_cmd == "archive":
        archive_and_upload(args.inpath, args.outpath, args.bucket,
                           access_token, secret_token, args.prefix, args.delimiter)


if __name__ == "__main__":
    main()
