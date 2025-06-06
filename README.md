# PROFESSIONAL SCRAPER

## Setting Up the Environment

To set up the virtual environment and install the required packages, follow these steps:

1. **Create a Virtual Environment:**

   Open your terminal and navigate to the project directory. Then, run the following command to create a virtual environment:

   ```bash
   python3 -m venv venv
   ```

2. **Activate the Virtual Environment:**

   - On macOS and Linux:

     ```bash
     source venv/bin/activate
     ```

   - On Windows:

     ```bash
     .\venv\Scripts\activate
     ```

3. **Install the Required Packages:**

   Once the virtual environment is activated, install the required packages using `requirements.txt`:

   ```bash
   pip install -r requirements.txt
   ```

Make sure you have Python 3 installed on your system. You can check your Python version by running `python3 --version` in your terminal. 

To run your individual csv file for scraping alumni from schools, the fields must be
name, url(something like gocrimson), last_seen_year, team, gender, school, graduation_year