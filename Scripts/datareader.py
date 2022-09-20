from typing import Tuple
import pandas as pd


class DataReader:

    def parse_filename(self, path: str) -> str:
        """Extract filename from file path

        Args:
            path (str): File path

        Returns:
            str: File name
        """
        return path.split('/')[2].split('.')[0]

    def generate_id(self, idx, name) -> str:
        """Generate an unique id to serve as a primary key between two tables

        Args:
            idx (_type_): The index of the row(row number)
            name (_type_): Name of the file

        Returns:
            str: Concatenation of file name and row index
        """
        return f"{name}_{idx}"

    def read_csv(self, filename: str) -> Tuple[list, list]:
        """Reads the csv file and returns the column headers and the rows in separate lists.

        Args:
            filename (str): Path to the csv file to read
        Returns:
            Tuple[list,list]: List of column headers and list of row entries
        """
        header = []
        rows = []
        with open(filename, 'r') as file:
            lines = file.readlines()
            header.extend(lines[0].strip('\n').split(';'))
            rows.extend(list(map(lambda line: line.strip('\n'), lines[1:])))

        return header, rows

    def extract_data(self, filepath: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """  Extract Data from csv and parse into pandas dataframe

        Args:
            filepath (str): Path of the csv file

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: Vehicle Information Dataframe, Trajectories Dataframe
        """

        vehicle_data = {
            "uid": [],
            "track_id": [],
            "type": [],
            "traveled_d": [],
            "avg_speed": []
        }

        trajectories_data = {
            "uid": [],
            "lat": [],
            "lon": [],
            "speed": [],
            "lon_acc": [],
            "lat_acc": [],
            "time": []
        }
        header, rows = self.read_csv(filepath)
        filename = self.parse_filename(filepath)
        for idx, row in enumerate(rows):
            uid = self.generate_id(idx, filename)
            line = row.split('; ')[:-1]
            vehicle_data["uid"].append(uid)
            vehicle_data["track_id"].append(int(line[0]))
            vehicle_data["type"].append(line[1])
            vehicle_data["traveled_d"].append(float(line[2]))
            vehicle_data["avg_speed"].append(float(line[3]))
            for i in range(0, (len(line) // 6)*6, 6):
                trajectories_data["uid"].append(uid)
                trajectories_data["lat"].append(float(line[4+i]))
                trajectories_data["lon"].append(float(line[4+i+1]))
                trajectories_data["speed"].append(float(line[4+i+2]))
                trajectories_data["lon_acc"].append(float(line[4+i+3]))
                trajectories_data["lat_acc"].append(float(line[4+i+4]))
                trajectories_data["time"].append(float(line[4+i+5]))

        vehicle_data_df = pd.DataFrame(vehicle_data).reset_index(drop=True)
        trajectories_data_df = pd.DataFrame(
            trajectories_data).reset_index(drop=True)

        return vehicle_data_df, trajectories_data_df
