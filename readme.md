# DOT: Digital Operation Tiwn

python3 -m venv .venv
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
pytest -q
uvicorn digital_operation_twin.main:app --reload

curl http://127.0.0.1:8000/api/healthz

python scripts/send_csv_to_api.py \
  --csv ./path/to/sample.csv \
  --customer customer_a



# GIT

git status
git remote -v
git remote add origin git@github-personal:nickmorris84/dot_version1.git

git checkout -b feature/dot-stabilisation
git branch

git add .
git commit -m "Add README, logging fixes, CSV test script"
