import pandas as pd
import matplotlib.pyplot as plt

folder = '/mnt/data_partition/Research/UCD01 data collection/data/batch3/raw/20221205/'
file_sensorlogger = f'{folder}/i7/sensorlogger/2022-12-05_03-34-12/Accelerometer.csv'
file_watch = f'{folder}/h2c4/watch/1670211253953_Ngan3_Badmintonserving_5.csv'

df_sensorlogger = pd.read_csv(file_sensorlogger)
df_watch = pd.read_csv(file_watch, header=None)
df_watch.columns = ['timestamp', 'gyr_x', 'gyr_y', 'gyr_z', 'acc_x', 'acc_y', 'acc_z']

# convert to millisecond
df_sensorlogger['time'] /= 1e6

fig = plt.figure()


def onclick(event):
    ix, iy = event.xdata, event.ydata
    print('x = %d, y = %d' % (ix, iy))


cid = fig.canvas.mpl_connect('button_press_event', onclick)

plt.subplot(2, 1, 1)
plt.title('sensorlogger')
plt.plot(df_sensorlogger['time'], df_sensorlogger[['x', 'y', 'z']])
plt.grid()

plt.subplot(2, 1, 2)
plt.title('watch')
plt.plot(df_watch['timestamp'], df_watch[['acc_x', 'acc_y', 'acc_z']])
plt.grid()

plt.show()
_ = 1
