## Quick Start
- Virtual environment and installation of dependencies.
```bash
cd /path/to/project/geodata
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

#### Environment Variables.
- We create the .env for the environment variables.
- Requests [API_KEY_CSC](https://countrystatecity.in/) in the "Requests API Key" section
```bash
# Inside `.env`.
API_KEY_CSC="..."
```
