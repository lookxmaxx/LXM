from flask import Flask, render_template, request, redirect, url_for, flash
import sqlite3
import pandas as pd  # Make sure you have pandas installed with: pip install pandas
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import os
import json


app = Flask(__name__, static_folder='static', template_folder='templates')


def connect_to_google_sheets():
    # Read the JSON credentials from the environment variable
    google_credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if google_credentials_json is None:
        raise Exception("GOOGLE_CREDENTIALS_JSON environment variable not set")

    creds_dict = json.loads(google_credentials_json)
    creds = Credentials.from_service_account_info(creds_dict)
    client = gspread.authorize(creds)

    sheet = client.open("LXM Creator Data").worksheet("Creators")
    return sheet


@app.route('/upload_csv', methods=['GET', 'POST'])
def upload_csv():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        
        file = request.files['file']
        
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        
        if file and file.filename.endswith('.csv'):
            try:
                # Read the CSV file
                df = pd.read_csv(file)

                # Check if the necessary columns exist
                if 'Link' not in df.columns or 'Views' not in df.columns:
                    flash('CSV must contain columns "Link" and "Views".')
                    return redirect(request.url)
                
                # Extract only the relevant columns
                filtered_df = df[['Link', 'Views']]
                
                # Connect to Google Sheets
                sheet = connect_to_google_sheets()
                all_data = sheet.get_all_values()
                headers = all_data[0]
                data = all_data[1:]

                # Create a dictionary to find rows by link
                link_to_row = {row[1]: index + 2 for index, row in enumerate(data)}

                for index, row in filtered_df.iterrows():
                    link = row['Link']
                    views = int(row['Views'])
                    
                    if link in link_to_row:
                        row_index = link_to_row[link]
                        
                        # Fetch CPM from the Google Sheet
                        cpm = float(sheet.cell(row_index, 5).value)
                        earnings = (views / 1000) * cpm
                        
                        # Update the sheet with Views and Earnings
                        sheet.update_cell(row_index, 6, views)  # Column F for Views
                        sheet.update_cell(row_index, 7, earnings)  # Column G for Earnings
                        
                flash('CSV uploaded and processed successfully!')
                return redirect(url_for('manager'))
            
            except Exception as e:
                flash(f'Error processing file: {e}')
                return redirect(request.url)
    
    return render_template('upload_csv.html')

def connect_to_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file("service_account.json", scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open("LXM Creator Data").worksheet("Creators")
    return sheet





# Create Database
def create_database():
    conn = sqlite3.connect('submissions.db')
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS submissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reel_link TEXT NOT NULL,
            submission_time TEXT NOT NULL,
            status TEXT DEFAULT 'Pending',
            rejection_reason TEXT DEFAULT ''
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS creators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            cpm INTEGER NOT NULL,
            email TEXT,
            dashboard_link TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS announcements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message TEXT NOT NULL,
            timestamp TEXT NOT NULL
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            creator_id INTEGER,
            message TEXT,
            timestamp TEXT,
            FOREIGN KEY (creator_id) REFERENCES creators(id)
        )
    ''')

    conn.commit()
    conn.close()
    
@app.route('/onboard_creator', methods=['POST'])
def onboard_creator():
    username = request.form['username']
    cpm = request.form['cpm']
    email = request.form.get('email', '')  # Optional field

    # Generate a unique ID for the creator
    creator_id = str(uuid.uuid4())[:8]

    conn = sqlite3.connect('submissions.db')
    cursor = conn.cursor()
    
    cursor.execute(''' 
        CREATE TABLE IF NOT EXISTS creators (
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            cpm INTEGER NOT NULL,
            email TEXT,
            dashboard_link TEXT
        )
    ''')

    # Create a dashboard link for the creator (Filtered Google Sheets Link)
    dashboard_link = f"https://docs.google.com/spreadsheets/d/1YspZP0TYMLRxerN7COtXCwRHnAQhWD0Dl7ZzW_aXrf4/edit#gid=0&f={username}"

    cursor.execute("INSERT INTO creators (id, username, cpm, email, dashboard_link) VALUES (?, ?, ?, ?, ?)",
                   (creator_id, username, cpm, email, dashboard_link))
    
    conn.commit()
    conn.close()
    
    return redirect(url_for('manager'))




@app.route('/')
def home():
    return redirect(url_for('manager'))


@app.route('/success')
def success():
    return render_template('success.html')

@app.route('/manager')
def manager():
    conn = sqlite3.connect('submissions.db')
    cursor = conn.cursor()

    filter_status = request.args.get('status', 'All')

    if filter_status == 'All':
        cursor.execute("SELECT * FROM submissions")
    else:
        cursor.execute("SELECT * FROM submissions WHERE status=?", (filter_status,))
    
    submissions = cursor.fetchall()

    cursor.execute("SELECT * FROM creators")
    creators = cursor.fetchall()
    
    conn.close()
    return render_template('manager.html', submissions=submissions, creators=creators, filter_status=filter_status)


@app.route('/update_cpm/<id>', methods=['POST'])  # Changed <int:id> to <id>
def update_cpm(id):  
    new_cpm = request.form['cpm']
    conn = sqlite3.connect('submissions.db')
    cursor = conn.cursor()
    cursor.execute("UPDATE creators SET cpm = ? WHERE id = ?", (new_cpm, id))
    conn.commit()
    conn.close()
    return redirect(url_for('manager'))


@app.route('/delete_creator/<id>', methods=['POST'])  # Changed <int:id> to <id>
def delete_creator(id):  
    conn = sqlite3.connect('submissions.db')
    cursor = conn.cursor()
    cursor.execute("DELETE FROM creators WHERE id=?", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for('manager'))

@app.route('/clear_data', methods=['GET'])
def clear_data():
    conn = sqlite3.connect('submissions.db')
    cursor = conn.cursor()
    
    # Clear all rows from your tables
    cursor.execute("DELETE FROM submissions")
    cursor.execute("DELETE FROM creators")
    cursor.execute("DELETE FROM announcements")
    cursor.execute("DELETE FROM notifications")
    
    conn.commit()
    conn.close()
    
    return "All data cleared successfully!"




# Route for displaying creator dashboard with announcements
@app.route('/creator/<int:creator_id>')
def creator_dashboard(creator_id):
    conn = sqlite3.connect('submissions.db')
    cursor = conn.cursor()
    
    # Fetch all announcements (in descending order so newest appears first)
    cursor.execute("SELECT message, timestamp FROM announcements ORDER BY id DESC")
    announcements = cursor.fetchall()
    
    conn.close()
    return render_template('creator_dashboard.html', creator_id=creator_id, announcements=announcements)

@app.route('/submit/<creator_id>', methods=['GET', 'POST'])
def submit(creator_id):
    if request.method == 'POST':
        reel_link = request.form['reel_link']
        submission_time = datetime.now().strftime("%Y-%m-%d %I:%M %p")
        
        conn = sqlite3.connect('submissions.db')
        cursor = conn.cursor()
        
        # Validate creator_id
        cursor.execute("SELECT id FROM creators WHERE id = ?", (creator_id,))
        creator_exists = cursor.fetchone()
        
        if not creator_exists:
            return "Invalid creator ID. Access Denied.", 403
        
        cursor.execute("INSERT INTO submissions (reel_link, submission_time, creator_id) VALUES (?, ?, ?)",
                       (reel_link, submission_time, creator_id))
        conn.commit()
        conn.close()
        
        return redirect(url_for('success'))
    
    return render_template('submit.html', creator_id=creator_id)

# Route for rereviewing a reel (Resetting status to Pending)
@app.route('/rereview/<int:id>', methods=['POST'])
def rereview(id):
    conn = sqlite3.connect('submissions.db')
    cursor = conn.cursor()
    cursor.execute("UPDATE submissions SET status='Pending', rejection_reason='' WHERE id=?", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for('manager'))


@app.route('/approve/<int:id>', methods=['POST'])
def approve(id):
    conn = sqlite3.connect('submissions.db')
    cursor = conn.cursor()
    
    cursor.execute("UPDATE submissions SET status='Approved' WHERE id=?", (id,))
    cursor.execute("SELECT creator_id, reel_link, submission_time FROM submissions WHERE id=?", (id,))
    submission = cursor.fetchone()

    # Fetch the CPM of the creator
    cursor.execute("SELECT username, cpm FROM creators WHERE id=?", (submission[0],))
    creator_data = cursor.fetchone()
    username = creator_data[0]
    cpm = creator_data[1]

    conn.commit()
    conn.close()

    if submission:
        try:
            worksheet = connect_to_google_sheets()
            # Update Google Sheets with properly formatted data
            worksheet.append_row([
                username, 
                submission[1], 
                submission[2], 
                'Approved', 
                cpm,  # This ensures the CPM is recorded
                "", # Views to be updated later via CSV import
                "", # Earnings to be calculated later
                ""  # Rejection Reason is blank since it's approved
            ])
        except Exception as e:
            print(f"Error updating Google Sheets: {e}")
    
    return redirect(url_for('manager'))

@app.route('/reject/<int:id>', methods=['POST'])
def reject(id):
    reason = request.form['reason']
    conn = sqlite3.connect('submissions.db')
    cursor = conn.cursor()
    cursor.execute("UPDATE submissions SET status='Rejected', rejection_reason=? WHERE id=?", (reason, id))
    cursor.execute("SELECT creator_id, reel_link, submission_time FROM submissions WHERE id=?", (id,))
    submission = cursor.fetchone()
    conn.commit()
    conn.close()
    
    if submission:
        try:
            worksheet = connect_to_google_sheets()
            worksheet.append_row([
                submission[0], 
                submission[1], 
                submission[2], 
                'Rejected', 
                "",  # CPM is empty for now, we'll update it in a moment
                "",  # Views to be updated later via CSV import
                "",  # Earnings will be calculated later
                reason  # Rejection Reason provided
            ])
        except Exception as e:
            print(f"Error updating Google Sheets: {e}")
    
    return redirect(url_for('manager'))



# Route for sending announcements (Manager Dashboard)
@app.route('/send_announcement', methods=['POST'])
def send_announcement():
    message = request.form['message']
    timestamp = datetime.now().strftime("%Y-%m-%d %I:%M %p")

    conn = sqlite3.connect('submissions.db')
    cursor = conn.cursor()
    cursor.execute("INSERT INTO announcements (message, timestamp) VALUES (?, ?)", (message, timestamp))
    conn.commit()
    conn.close()

    return redirect(url_for('manager'))

if __name__ == '__main__':
    if not os.path.exists('submissions.db'):
        print("Database not found. Creating new database...")
        create_database()
    else:
        print("Database already exists. Skipping creation step.")

    app.run(host='0.0.0.0', port=10000)
