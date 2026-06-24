"""
PHI Discovery Profiling — patients.csv
Generates a Sweetviz HTML report to identify PHI columns before de-identification.
Run this before the Glue de-id ETL job as part of the HIPAA audit trail.
"""

import pandas as pd
import sweetviz as sv

df = pd.read_csv(
    '/Users/sayantaninath/output/csv/patients.csv',
    on_bad_lines='skip',
    nrows=5000  # sample for faster profiling; remove for full scan
)

report = sv.analyze(df)
report.show_html('/Users/sayantaninath/Downloads/patients_profile.html')
print("Profile saved to ~/Downloads/patients_profile.html")
