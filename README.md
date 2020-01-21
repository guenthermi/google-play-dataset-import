# google-play-dataset-import
This script imports data from a Google Play Store Apps dataset to a PostgreSQL database.

# Run the script

First, you have to change the configuration in `db_config.json` according to your PostgreSQL server.
Afterward, you might need to install some python packages (pandas, psycopg2).
Then you have to download and unzip the dataset from the (Kaggle Website)[https://www.kaggle.com/lava18/google-play-store-apps/data].
Finally, you can run the `loader.py` script to import the data.

```
python3 loader.py <path/to/your/dataset/folder>
```
