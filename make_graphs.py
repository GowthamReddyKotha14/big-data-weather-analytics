import matplotlib.pyplot as plt
import json
import os
import glob

# Read Task02 JSON output copied from Docker
output_path = "task02"
json_files = glob.glob(f"{output_path}/part-*.json")

records = []
for f in json_files:
    with open(f, "r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if line:
                records.append(json.loads(line))

if not records:
    raise ValueError("No Task02 JSON records found in ./task02")

# Sort by year
records.sort(key=lambda x: x["year"])

years = [r["year"] for r in records]
min_temps = [r["min_temp"] for r in records]
max_temps = [r["max_temp"] for r in records]
mean_temps = [r["mean_temp"] for r in records]
median_temps = [r["median_temp"] for r in records]

os.makedirs("graphs", exist_ok=True)

# Min temperature graph
plt.figure()
plt.plot(years, min_temps)
plt.title("Minimum Temperature by Year")
plt.xlabel("Year")
plt.ylabel("Temperature (°C)")
plt.savefig("graphs/min_temp_by_year.png", bbox_inches="tight")
plt.close()

# Max temperature graph
plt.figure()
plt.plot(years, max_temps)
plt.title("Maximum Temperature by Year")
plt.xlabel("Year")
plt.ylabel("Temperature (°C)")
plt.savefig("graphs/max_temp_by_year.png", bbox_inches="tight")
plt.close()

# Mean temperature graph
plt.figure()
plt.plot(years, mean_temps)
plt.title("Mean Temperature by Year")
plt.xlabel("Year")
plt.ylabel("Temperature (°C)")
plt.savefig("graphs/mean_temp_by_year.png", bbox_inches="tight")
plt.close()

# Median temperature graph
plt.figure()
plt.plot(years, median_temps)
plt.title("Median Temperature by Year")
plt.xlabel("Year")
plt.ylabel("Temperature (°C)")
plt.savefig("graphs/median_temp_by_year.png", bbox_inches="tight")
plt.close()

print("Real graphs created from Task02 output!")