import matplotlib.pyplot as plt
import pandas as pd

import tikzplotlib

fig, axs = plt.subplots(2)

df1 = pd.read_csv("implementation_cpu_usage.csv")
df1 = df1.drop("time", axis=1)
df2 = pd.read_csv("implementation_ram_usage.csv")
df2 = df2.drop("time", axis=1)

df1.plot(ax=axs[0])
df2.plot(ax=axs[1])

axs[0].set_ylabel("Normalized CPU usage")
axs[1].set_ylabel("Normalized RAM usage")

axs[1].set_xlabel("Time (s)")

plt.savefig("implementation_results.pdf")
tikzplotlib.save("implementation_results.tex")