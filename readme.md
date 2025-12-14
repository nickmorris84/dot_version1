# DOT: Digital Operation Tiwn

python3 -m venv .venv
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
pytest -q

uvicorn digital_operation_twin.main:app --reload
DOT_LOG_LEVEL=DEBUG uvicorn digital_operation_twin.main:app --reload --log-level debug

export DOT_LOG_LEVEL=DEBUG
python scripts/run_ingest_tests.py --customer-id customer_a


curl http://127.0.0.1:8000/api/healthz

export DOT_LOG_LEVEL=DEBUG
python scripts/send_csv_to_api.py \
  --csv ./data/inputs/credit_card_process_activities.csv \
  --customer customer_a




# GIT

git status
git remote -v
git remote add origin git@github-personal:nickmorris84/dot_version1.git

git checkout -b feature/dot-stabilisation
git branch

git add .
git commit -m "Add README, logging fixes, CSV test script"
