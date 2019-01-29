# What is the IPS?
The International Passenger Survey (IPS) collects monthly information relating to overseas travel.

# What does the current IPS System do?
The current IPS system calculates weighting factors and imputes missing data for specified periods, using statistical configurations supplied and managed by the users IPS team within Social Surveys Division (SSD).

# Functionality
This package supports the calculations within IPS System. This is also referred to as the "Main Run".
Users select which steps should be executed and then generate reports from the results. These steps can be found under _calculations_ in the **main** folder.

All data resides in the database and code connecting to the database can be found in the _io_ folder. Finally any generic functions are located in the _utils_ folder.

# How to use this package?


# Quick Start

1. Clone and navigate to the root directory of this repository.

2. Ensure python 3 and pip are installed.

3. Install the dependencies with `pip install -r requirements.txt`

4. Call the *main* function or

4a. run all the tests using pytest (see for configuring pytest in pycharm and running tests)