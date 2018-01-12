import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3

# period for the moving average
period = 20000

# connect to SQL database 'tickdata.db'
conn = sqlite3.connect('ticks.db')

# load data from SQL database
bfx_data = pd.read_sql_query("""SELECT * FROM bitfinex;""", conn)
krk_data = pd.read_sql_query("""SELECT * FROM kraken;""", conn)

# dissconnect from database
conn.commit()
conn.close()

# calculate midpoint between bid and ask for each asset
bfx_data['mid'] = (bfx_data['bid'] + bfx_data['ask']) / 2
krk_data['mid'] = (krk_data['bid'] + krk_data['ask']) / 2

# create a timestamp array with 1 second intervals
total_rows = np.shape(bfx_data['timestamp'])[0]
timestamp_min = int(bfx_data['timestamp'][0])
timestamp_max = int(bfx_data['timestamp'][total_rows - 1])
timestamps = pd.DataFrame({'timestamp':range(timestamp_min , timestamp_max, 1)})

# create complete arrays
bfx_complete = pd.merge(timestamps, bfx_data,
                        how='left', left_on='timestamp', right_on='timestamp'
                        ).drop(['id', 'bid', 'ask'], axis=1)
krk_complete = pd.merge(timestamps, krk_data,
                        how='left', left_on='timestamp', right_on='timestamp'
                        ).drop(['id', 'bid', 'ask'], axis=1)

# interpolate missing values with linear interpolation
bfx_complete = bfx_complete.interpolate(method='linear')
krk_complete = krk_complete.interpolate(method='linear')

# calculate spread
spread = krk_complete['mid'] - bfx_complete['mid']

# plot spread
plt.plot(spread)

# calculate moving average and standard deviation
moving_average = spread.rolling(window=period, center=False).mean()
moving_sd = spread.rolling(window=period, center=False).std()

# standardise spread
moving_z = (spread - moving_average) * (1/moving_sd)

# plot standardised spread
plt.plot(moving_z)

# plot spread and moving average
ax1 = plt.subplot(211)
plt.plot(spread)
plt.plot(moving_average)

# plot standardised spread and standard deviation bars
ax2 = plt.subplot(212, sharex=ax1)
plt.plot(moving_z)
plt.axhline(0, color='black')
plt.axhline(4.0, color='red', linestyle='--')
plt.axhline(-4.0, color='green', linestyle='--')

