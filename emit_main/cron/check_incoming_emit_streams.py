import argparse
import os
import datetime


def is_folder_empty(folder_path):
    return not any(os.scandir(folder_path))


def files_modified_within_minutes(folder_path, minutes):
    current_time = datetime.datetime.now()
    time_threshold = current_time - datetime.timedelta(minutes=minutes)

    try:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                modified_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))

                if modified_time >= time_threshold:
                    return True

    except Exception as e:
        print(f"Error: {e}")
        return False

    return False


def main():

    parser = argparse.ArgumentParser(description="Report when no new data has been seen since number of minutes")
    parser.add_argument("-m", "--minutes", default="240", help="Minutes threshold", type=int)
    parser.add_argument("-s", "--streams", default="bad,1674,1675,1676", help="Stream folders to check")
    args = parser.parse_args()

    minutes = args.minutes

    for stream in args.streams.split(","):
        current_date = datetime.datetime.now()
        looking_for_data = True
        count = 0
        while looking_for_data and count < 10:
            formatted_date = current_date.strftime("%Y%m%d")
            folder_path = f"/store/emit/ops/data/streams/{stream}/{formatted_date}/raw"
            print(f"- Checking {folder_path}")
            # Check if empty and continue to next iteration if so
            if not os.path.exists(folder_path) or is_folder_empty(folder_path):
                print(f"  - Folder {folder_path} doesn't exist or is empty. Going back a day...")
                current_date = current_date - datetime.timedelta(days=1)
                count += 1
                continue
            # If not empty, then check data
            if files_modified_within_minutes(folder_path, minutes):
                print(f"  - There are files modified within the last {minutes} minutes.")
            else:
                print(f"  - ERROR: There are no files modified within the last {minutes} minutes.")
            looking_for_data = False


if __name__ == "__main__":
    main()
