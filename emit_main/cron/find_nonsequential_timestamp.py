import sys

file = sys.argv[1]
print(file)
with open(file, "r") as f:
    lines = f.readlines()
    prev_time = 0
    for line in lines:
        timestamp = int(line.split(" ")[1])
        if timestamp < prev_time:
            print(f"Bad timestamp: {timestamp}")
            break
        prev_time = timestamp