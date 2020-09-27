A basic lock server over HTTP to coordinate processes with HTTP basic authentication support.
Implements exclusive and shared lock types - there may only be 1 exclusive lock which prevents any shared lock acquisitions, but multiple shared locks may co-exist.
Should require only standard library dependencies (Tested on python 3.7, some care taken to work on older versions).

http_lock_server.py -i <interval> -p <port> [-a <username:password>] [-c <certificate_path>]

To run tests be in root directory:
python3 -m unittest discover -v [-p <test_filename>]
