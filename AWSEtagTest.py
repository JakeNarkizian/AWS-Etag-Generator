import io
import os
import traceback
import unittest
import uuid
import StringIO
import boto
import itertools
from boto.s3.key import Key
from AWSEtag import getEtag

# NOTE: test will fail if part size on test does not match main script partsize
part_size = 2**20 * 50

class AWSEtagTest(unittest.TestCase):
    def setUp(self):
        s3 = boto.connect_s3()
        self.bucket = s3.create_bucket('awsetagtest_%s' % str(uuid.uuid4()))
        self.cleanUpQueue = {self.bucket: self._delete_bucket}  # pair of entity and delete method
        self.keys = []

        # Change size in this for loop to create other tests
        for size in [1, 2**20 * 50]:#, 2**20 * 50 + 1, int(2**20 * 50 * 2.1)]:
            self.keys.append(self._upload_to_key(size))

    def tearDown(self):
        for thing, cleanUp in self.cleanUpQueue.items():
            cleanUp(thing)

    def test(self):
        def testKey(key):
            filename = "%ssize_%s" % (key.size, uuid.uuid4())
            self.cleanUpQueue[filename] = os.remove
            key.get_contents_to_filename(filename)
            self.assertEqual(getEtag(filename), key.etag[1:-1])

        self._runTestWithParams(testKey, self.keys)


    def _runTestWithParams(self, testFunc, params):
        """
        Runs test with each of the parameters in the list. If there is a failure
        it is captured and the rest of the parameters are run. When  the test
        function has been run with all paramters the tracebacks of the failed
        tests are printed.

        :param function testFunc: function to run for test
        :param list params: a list of parameters
        """
        errors = {}
        for prm in params:
            try:
                testFunc(prm)
            except:
                f = StringIO.StringIO()
                traceback.print_exc(file=f)
                errors[prm] = f.getvalue()

        if len(errors) != 0:
            m = "This test contains %d subtests %d of which failed" % (len(params), len(errors))

            for prm, stackTraceStr in errors.items():
                itemBody = "FAIL: %s%s" % (testFunc.__name__, str(prm))
                itemHead = "\n\n %s \n " % ('='*(len(itemBody)+1))
                itemFoot = "\n %s \n" % ('-'*(len(itemBody)+1))

                m += itemHead + itemBody + itemFoot + ' ' + stackTraceStr.replace('\n', '\n ')

            self.fail(msg=m)

    def _delete_bucket(self, bucket):
        s3 = boto.connect_s3()
        for key in bucket:
            key.delete()
        s3.delete_bucket(self.bucket)

    def _upload_to_key(self, size):
        key = Key(self.bucket)
        key.key = '%.0fMiB_file_%s' % (float(size/(2**20)), str(uuid.uuid4()))
        with open('/dev/urandom', 'r') as f:
            mp = self.bucket.initiate_multipart_upload(key_name=key.name)
            start = 0
            partNum = itertools.count()
            try:
                while start < size:
                    end = min(start + part_size, size)
                    fPart = io.BytesIO(f.read(part_size))
                    mp.upload_part_from_file(fp=fPart,
                                             part_num=next(partNum) + 1,
                                             size=end - start)
                    start = end
                    if start == size:
                        break

                assert start == size
            except:
                mp.cancel_upload()
                raise
            else:
                mp.complete_upload()
        return key
