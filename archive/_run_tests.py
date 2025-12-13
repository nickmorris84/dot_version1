%load_ext autoreload
%autoreload 2


from config.config_loader import load_yaml, deep_merge
from pathlib import Path


base_cfg = load_yaml("config/base.yaml")
cust_cfg = load_yaml("config/customer_a.yaml")

final_cfg = deep_merge(base_cfg, cust_cfg)

final_cfg