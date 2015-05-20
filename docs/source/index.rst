.. aiohdfs documentation master file, created by
   sphinx-quickstart on Tue May 19 17:22:48 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to aiohdfs's documentation!
===================================


aioHDFS is a Python wrapper for the Hadoop WebHDFS REST API. This is based on PyWebHDFS.

Many of the current Python HDFS clients rely on Hadoop Streaming which
requires Java to be installed on the local machine. The other option for
interacting with HDFS is to use the WebHDFS REST API. The purpose of
this project is to simplify interactions with the WebHDFS API. The
AioHDFS client will implement the exact functions available in the
WebHDFS REST API and behave in a manner consistent with the API.

Example::

     from aiohdfs import Client
     import asyncio
     loop = asyncio.get_event_loop()
     hdfs = Client(host='namenode', port='50070',user_name='hdfs')
     loop.run_until_complete(hdfs.make_dir("/user/hdfs/thingy"))




Contents::

 .. toctree::
    :maxdepth: 2
 .. autoclass:: aiohdfs.Client
    :members:  __init__, create_file, append_file, read_file, make_dir, rename_file_dir, delete_file_dir, get_file_dir_status, get_file_checksum, list_dir


Installation
------------

Install aiohdfs by running:

    python setup.py install

Contribute
----------

- Issue Tracker: https://github.com/RiverlandReply/aiohdfs/issues
- Source Code: https://github.com/RiverlandReply/aiohdfs

Support
-------

If you are having issues, please let us know on the github page.

License
-------

The project is licensed under the BSD license.
