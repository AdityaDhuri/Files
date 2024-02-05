import pandas as pd
import os
from datetime import datetime
import logging
from  adapter_pattern.base_classes.sftp2 import SFTPAdapter

if __name__ == "__main__":
    try:
        config = pd.read_json(r"C:\Users\aditya.dhuri\Desktop\True_fit_and_hill\SFTP\config\config.json")
        logs_folder = None
        for index, config_item in config.iterrows():

            if config_item.get("type") == "generic":
                logs_folder = config_item.get("logs_path")
                print(logs_folder)

                # logs_folder = config[config["type"] == "generic"].iloc[0].get("logs_path")
                config = config.drop(index).reset_index(drop=True)
                config = config.iloc[0]

                log_folder = os.path.join(logs_folder, 'logs')
                os.makedirs(log_folder, exist_ok=True)

                today_subfolder = os.path.join(log_folder, f'{datetime.now().strftime("%Y-%m-%d")}.log')
                logging.basicConfig(filename=today_subfolder, level=logging.INFO,
                                    format='%(asctime)s - %(levelname)s - %(message)s')
                break
            else :
                raise ValueError("logs folder not found in the config file")

    except Exception as e:
        print(f"Error during initialization: {e}")
        logging.error(f"Error during initialization: {e}")

    sftp_adapter = SFTPAdapter()
    sftp_adapter.parameters_sftp(config)
    sftp_adapter.connect()
    sftp_adapter.execute()
    sftp_adapter.disconnect()
