import pandas as pd
import datetime as dt
import requests, json, os, sys, base64
from azure.storage.blob import BlockBlobService, ContentSettings
from io import StringIO

class Flurry():
    def __init__(self):
        with open(os.path.join(sys.path[0], "config.json"), "r") as f:
            self.config = json.loads(f.read())
    
    def get_last_date(self, df):
        if df is not None:
            last_date = df['date'].max()
            last_date = dt.datetime.strptime(last_date, "%Y-%m-%d")
        else:
            last_date = None
        return(last_date)

    def get_param_dateTime(self, last_date):
        if last_date is not None:
            ini_date = last_date + dt.timedelta(days = -1)
            ini_date = dt.datetime.strftime(ini_date, "%Y-%m-%d")
        else:
            ini_date = self.config['default_ini_date']
        fin_date = dt.datetime.today().strftime("%Y-%m-%d")
        param = ini_date + "/" + fin_date
        return(param)

    def get_data(self, param_dateTime, url):
        r = requests.get(url, headers={"content-type": "application/json", "Authorization": "Bearer " + self.config['flurry_key']}, 
        params={"metrics": "newDevices,activeDevices,completeSessions,activeUsers", 
        "dateTime":param_dateTime,
        })
        r = json.loads(r.content)
        df = pd.DataFrame.from_dict(r['rows'])
        df['key'] = df['app|name'] + "_" + df['platform|name'] + "_" + df['dateTime']
        df['dateTime'] = pd.to_datetime(df['dateTime'])
        df['date'] = df['dateTime'].dt.strftime("%Y-%m-%d")
        df = df.sort_values('dateTime')
        return(df)
    
    def get_df_from_blob(self):
        try:
            block_blob_service = BlockBlobService(
                account_name=self.config['blob_account_name'], 
                account_key=self.config['blob_account_key'])
            blob = block_blob_service.get_blob_to_text(
                container_name=self.config['blob_image_container'],
                blob_name=self.config['filename']).content
            return(self.blob_to_df(blob))
        except Exception as e:
            print(e)
            return(None)

    def blob_to_df(self, blob):
        df = pd.read_csv(StringIO(blob))
        return(df)

    def save_csv(self, df):
        df.to_csv(os.path.join(self.config['local_path'], self.config['filename']), index=False)

    def save_bkp(self, df):
        date = dt.datetime.now()
        time = date.strftime("%H%M%S")
        date = date.strftime("%Y%m%d") 
        filename = date + "_" + time + "_" "bkp_flurry"
        if df is not None:
            self.save_in_blob(df, filename)
            
    def save_in_blob(self, df, filename):
        data = df.to_csv(index=False, encoding='utf-8')
        try:
            block_blob_service = BlockBlobService(
                account_name=self.config['blob_account_name'], 
                account_key=self.config['blob_account_key'])
            block_blob_service.create_blob_from_text(self.config['blob_image_container'], filename, data)
        except Exception as e:
            print(e)

    def get_blobs_list(self):
        try:
            block_blob_service = BlockBlobService(
                account_name=self.config['blob_account_name'], 
                account_key=self.config['blob_account_key'])
            blobs_list = block_blob_service.list_blobs(
                container_name=self.config['blob_image_container'])
            return(blobs_list)
        except Exception as e:
            print(e)

    def list_blobs(self):
        try:
            blobs_list = self.get_blobs_list()
            for blob in blobs_list:
                print(blob.name)
        except Exception as e:
            print(e)
    
    def remove_blob(self, blob):
        try:
            block_blob_service = BlockBlobService(
                account_name=self.config['blob_account_name'],  
                account_key=self.config['blob_account_key'])
            block_blob_service.delete_blob(
                container_name=self.config['blob_image_container'],
                blob_name=blob
            )
        except Exception as e:
            print(e)

    def remove_backup_files(self):
        try:
            blobs_list = self.get_blobs_list()
            for blob in blobs_list:
                if blob.name[-10:] == "bkp_flurry":
                    self.remove_blob(blob.name)
        except Exception as e:
            print(e)

if __name__ == '__main__':
    try:
        flurry = Flurry()
        current_df = flurry.get_df_from_blob()
        last_date = flurry.get_last_date(current_df)
        param_dateTime = flurry.get_param_dateTime(last_date)
        new_df = flurry.get_data(param_dateTime, flurry.config['flurry_url'])
        concat_df = pd.concat([current_df, new_df])
        concat_df = concat_df.drop_duplicates(subset='key', keep='first')
        flurry.remove_backup_files()
        flurry.save_bkp(current_df)
        flurry.save_in_blob(concat_df, flurry.config['filename'])
        flurry.list_blobs()
    except Exception as e:
        print(e)





