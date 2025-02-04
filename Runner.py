#from libraries import check_requirements
import os
import subprocess
import schedule
import time
import threading
import platform
import json
from Kibana import kibana  # Ensure this matches your file naming (case sensitivity matters)

def run_code():
    kb = kibana(wh="DSM5", slack_url="https://hooks.slack.com/triggers/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", data_needed="Sort", FHDs=8)

    result, response =kb.pull_kibana()

    print("Kibana Response")
    print(json.dumps(result, indent =2))

if __name__ == "__main__":
    run_code()
