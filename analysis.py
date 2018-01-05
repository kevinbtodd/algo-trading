import pandas as pd
import matplotlib.pyplot as plt
import sqlite3

# connect to sqlite database 'tickdata.db'
conn = sqlite3.connect('ticks.db')

# cex_data = pd.read_sql_query("""SELECT id, bid, ask FROM cex WHERE id > 268307 ORDER BY id;""", conn)
bfx_data = pd.read_sql_query("""SELECT * FROM bitfinex;""", conn)
krk_data = pd.read_sql_query("""SELECT * FROM kraken;""", conn)

conn.commit()
conn.close()

long_bfx = -bfx_data['ask'] + krk_data['bid']
# short_bfx = bfx_data['bid'] - cex_data['ask']

ma = pd.rolling_mean(long_bfx, 5000)
moving_sd = pd.rolling_std(long_bfx, 5000) 


moving_z = (long_bfx - ma) * (1/moving_sd)


plt.plot(moving_z)



ax1 = plt.subplot(211)
plt.plot(long_bfx)
plt.plot(ma)

ax2 = plt.subplot(212, sharex=ax1)
plt.plot(moving_z)
plt.axhline(0, color='black')
plt.axhline(4.0, color='red', linestyle='--')
plt.axhline(-4.0, color='green', linestyle='--')
