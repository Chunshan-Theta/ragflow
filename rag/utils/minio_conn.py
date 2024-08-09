import os
import time
from minio import Minio
from io import BytesIO
from rag import settings
from rag.settings import minio_logger
from rag.utils import singleton
import requests
from requests.auth import HTTPBasicAuth

class MinioAdminAPI(object):
    def __init__(self, host, access_key, secret_key):
        self._headers = {
            'Content-Type': 'application/json'
        }
        self._endpoint = f"http://{host}/minio/admin/v3/"
        self._access_key = access_key
        self._secret_key = secret_key

    # Set the quota using the Admin API
    def set_bucket_quota(self, bucket_name, quota_size):
        url = f"{self._endpoint}set-bucket-quota?bucket={bucket_name}"
        payload = {
            "quota": {
                "hard": f"{quota_size}"
            }
        }
        response = requests.post(
            url,
            json=payload,
            headers=self._headers,
            auth=HTTPBasicAuth(self._access_key, self.secret_key)
        )
        
        if response.status_code == 200:
            minio_logger.info(f"Quota of {quota_size} set for bucket '{bucket_name}'.")
        else:
            minio_logger.error(f"Failed to set quota: {response.text}")





@singleton
class RAGFlowMinio(object):
    def __init__(self):
        self.conn = None
        self.minioAdminApi = MinioAdminAPI(
            settings.MINIO["host"],
            access_key=settings.MINIO["user"],
            secret_key=settings.MINIO["password"],
        )
        self.__open__()

    def __open__(self):
        try:
            if self.conn:
                self.__close__()
        except Exception as e:
            pass

        try:
            self.conn = Minio(settings.MINIO["host"],
                              access_key=settings.MINIO["user"],
                              secret_key=settings.MINIO["password"],
                              secure=False
                              )
        except Exception as e:
            minio_logger.error(
                "Fail to connect %s " % settings.MINIO["host"] + str(e))

    def __close__(self):
        del self.conn
        self.conn = None
    @classmethod
    def _create_bucket(cls, bucket):
        if not cls.conn.bucket_exists(bucket):
            cls.conn.make_bucket(bucket)
            self.minioAdminApi.set_bucket_quota(bucket, "1GB")


    def health(self):
        bucket, fnm, binary = "txtxtxtxt1", "txtxtxtxt1", b"_t@@@1"
        if not self.conn.bucket_exists(bucket):
            self.conn.make_bucket(bucket)
        r = self.conn.put_object(bucket, fnm,
                                 BytesIO(binary),
                                 len(binary)
                                 )
        return r

    def put(self, bucket, fnm, binary):
        for _ in range(3):
            try:
                if not self.conn.bucket_exists(bucket):
                    self.conn.make_bucket(bucket)

                r = self.conn.put_object(bucket, fnm,
                                         BytesIO(binary),
                                         len(binary)
                                         )
                return r
            except Exception as e:
                minio_logger.error(f"Fail put {bucket}/{fnm}: " + str(e))
                self.__open__()
                time.sleep(1)

    def rm(self, bucket, fnm):
        try:
            self.conn.remove_object(bucket, fnm)
        except Exception as e:
            minio_logger.error(f"Fail rm {bucket}/{fnm}: " + str(e))

    def get(self, bucket, fnm):
        for _ in range(1):
            try:
                r = self.conn.get_object(bucket, fnm)
                return r.read()
            except Exception as e:
                minio_logger.error(f"fail get {bucket}/{fnm}: " + str(e))
                self.__open__()
                time.sleep(1)
        return

    def obj_exist(self, bucket, fnm):
        try:
            if self.conn.stat_object(bucket, fnm):return True
            return False
        except Exception as e:
            minio_logger.error(f"Fail put {bucket}/{fnm}: " + str(e))
        return False


    def get_presigned_url(self, bucket, fnm, expires):
        for _ in range(10):
            try:
                return self.conn.get_presigned_url("GET", bucket, fnm, expires)
            except Exception as e:
                minio_logger.error(f"fail get {bucket}/{fnm}: " + str(e))
                self.__open__()
                time.sleep(1)
        return


MINIO = RAGFlowMinio()


if __name__ == "__main__":
    conn = RAGFlowMinio()
    fnm = "/opt/home/kevinhu/docgpt/upload/13/11-408.jpg"
    from PIL import Image
    img = Image.open(fnm)
    buff = BytesIO()
    img.save(buff, format='JPEG')
    print(conn.put("test", "11-408.jpg", buff.getvalue()))
    bts = conn.get("test", "11-408.jpg")
    img = Image.open(BytesIO(bts))
    img.save("test.jpg")
