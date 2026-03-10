# MaintenanceToolbox

MaintenanceToolbox is a Streamlit web app for multi-organization shutdown planning, REX, and adherence dashboards.

## Included in this MVP

- Global admin account
- Organization + user management
- First-login password change
- French / English / Dutch interface
- CSV upload with per-organization field mapping
- Planning creation wizard
- Team setup by workshop
- OT selection with warning if an OT was not completed in a previous planning
- Manual operations integrated as internal tasks
- Planning generation with team assignment and simple precedence handling
- REX by OT (done / not done, cause, optional comment)
- Weekly adherence dashboard
- HTML print/export

## Default admin account

- Email: `admin@admin.admin`
- Password: `Admin123!`

Change it at first login.

## Local run

```bash
python -m venv .venv
source .venv/bin/activate  # Linux / macOS
# .venv\Scripts\activate   # Windows
pip install -r requirements.txt
streamlit run app.py
```

## Database

The app uses the `DATABASE_URL` environment variable.

Examples:

### SQLite (quick local test)

```bash
export DATABASE_URL="sqlite:///maintenance_toolbox.db"
```

### Supabase Postgres

Use the direct Postgres connection string from Supabase, for example:

```bash
export DATABASE_URL="postgresql+psycopg://USER:PASSWORD@HOST:6543/postgres?sslmode=require"
```

## Deployment recommendation

Recommended test deployment:

- **Database**: Supabase Postgres
- **App hosting**: Streamlit Community Cloud

## Notes

- Uploaded CSV files are stored temporarily in the database until the planning start time. On app usage after the shutdown start time, the app clears the stored CSV bytes automatically.
- For production usage with higher traffic, consider moving from this MVP to a more structured backend/API architecture.
