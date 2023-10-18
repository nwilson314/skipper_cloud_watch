import csv
import sys


if __name__ == "__main__":
    file_path = sys.argv[1]
    with open(file_path, newline='') as f:
        reader = csv.reader(f, delimiter=',')
        file = []
        for row in reader:
            file.append(row)

        header = file[0]
        latencies = []
        for i in range(1, len(file)):
            for j, col in enumerate(file[i]):
                if header[j] == "latency":
                    latencies.append(int(col))

        total = len(latencies)
        print(f'Total Number: {total}')
        number_above_1s = [x for x in latencies if x >= 1000]
        print(f"Number above 1s: {len(number_above_1s)}, {len(number_above_1s)/total*100}%")
        number_above_2s = [x for x in latencies if x >= 2000]
        print(f"Number above 2s: {len(number_above_2s)}, {len(number_above_2s)/total*100}%")
        number_above_5s = [x for x in latencies if x >= 5000]
        print(f"Number above 5s: {len(number_above_5s)}, {len(number_above_5s)/total*100}%")
        number_above_10s = [x for x in latencies if x >= 10000]
        print(f"Number above 10s: {len(number_above_10s)}, {len(number_above_10s)/total*100}%")