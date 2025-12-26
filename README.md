# Google Sheets Updater Service

This service downloads a CSV file from URL and rewrites the contents of a Google Spreadsheet.  
It is designed to run as a **systemd service** triggered by a **systemd timer**.

---

## Installation

1. Clone the repo:
   ```bash
   git clone https://github.com/YOUR/repo.git
   cd repo
   ```

   Create a Python virtual environment and install dependencies:

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```
2. Prepare your service file:

    Copy the example template:
    ```bash 
    cp file-uploader.service-example file-uploader.service
    ```
    Edit *file-uploader.service*:

    - Set GOOGLE_SHEET_ID (from the spreadsheet URL).

    - Set ENDPOINTS (your  API URLs, separated with ; ).

    - Set SHEETS (sheet names separated with ***;*** Sheet count should match endpoint count)

    - Adjust User and WorkingDirectory to your environment.

    Do not commit your .service file — it is already .gitignored.

3. Run the installer:

    ```bash
    chmod +x install-service.sh
    ./install-service.sh
    ```
4. Verify installation:

    ```bash
    systemctl status file-uploader.service
    systemctl list-timers | grep file-uploader
    ```
5. Logs

    To watch live logs:
    ```bash
    journalctl -u file-uploader.service -f
    ```
6. Updating configuration

    If you change your .service file, reinstall it:
    ```bash
    sudo cp file-uploader.service /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl restart file-uploader.timer
    ```