import os 
import joblib 
rf_path = os.path.join('./artifacts', 'rf_pipeline.joblib')
if joblib.load(rf_path):
    print("model loaded")
else:
    print("model not loaded")
