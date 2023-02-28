import pandas as pd
import matplotlib.pyplot as plt

folder = '/mnt/data_drive/projects/UCD01 - Privacy preserving data collection/data/batch3/raw/20230226/'
file_sensorlogger = f'{folder}/i7/sensorlogger/2023-02-26_11-25-51/AccelerometerUncalibrated.csv'
file_watch = f'{folder}/h2c4/watch/1677410740248_Ngan2_Badmintonserving_5.csv'

df_sensorlogger = pd.read_csv(file_sensorlogger)
df_watch = pd.read_csv(file_watch, header=None)
df_watch.columns = ['timestamp', 'gyr_x', 'gyr_y', 'gyr_z', 'acc_x', 'acc_y', 'acc_z']

# convert to millisecond
df_sensorlogger['time'] /= 1e6

fig = plt.figure()


def onclick(event):
    ix, iy = event.xdata, event.ydata
    subplot_title = event.inaxes.title.get_text()
    print('%s x = %d, y = %d' % (subplot_title, ix, iy))


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
