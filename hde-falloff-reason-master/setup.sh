#!/bin/bash

python -m venv venv
source venv/Scripts/activate
pip install -r requirements.txt
python -m spacy download en_core_web_sm

# Create kernel for venv
pip install ipykernel
python -m ipykernel install --name=hde_falloff_reason
