import argparse
import pandas as pd
import matplotlib.pyplot as plt

"""

Histogram:

python3 ./plot-event-metrics.py -i emetrics.csv -o plot.png --hist=time-in-queue --bins=5
python3 ./plot-event-metrics.py -i emetrics.csv -o plot.png --hist=time-in-service --bins=10

Scatterplot
python3 ./plot-event-metrics.py -i emetrics.csv -o plot.png --scat=arrival-time,response-time
"""

def main(args):
  df = pd.read_csv(args.i)

  # handle histogram
  if args.hist:
    column_name = args.hist

    if column_name not in df.columns:
      print(f"{column_name} does not exist in data frame.")
      return

    plt.figure()
    plt.hist(df[column_name], bins=args.bins)
    plt.xlabel(column_name)
    plt.ylabel("Frequency")
    plt.title(f"Histogram of {column_name}")
    plt.savefig(args.o)
    return
  
  # handle histogram
  if args.scat:
    column1, column2 = args.scat.split(",")
    for col in (column1, column2):
      if col not in df.columns:
        print(f"{col} does not exsit in frame.")
        return

    plt.figure()
    plt.scatter(df[column1], df[column2])
    plt.xlabel(column1)
    plt.ylabel(column2)
    plt.title(f"Scatterplot of {column1} vs {column2}")
    plt.savefig(args.o)
    return


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-i", help="Input file path")
  parser.add_argument("-o", help="Output file path")
  parser.add_argument("--hist", help="Want histogram")
  parser.add_argument("--bins", type=int, help="bin sizes")
  parser.add_argument("--scat", help="-scat=arrival-time,response-time")
  args = parser.parse_args()
  main(args)
