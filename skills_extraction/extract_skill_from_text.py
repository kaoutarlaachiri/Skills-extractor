from utils import extract_data_from_database3,upload_results
from skills_extraction.constants import SKILLS_SET,config



results = extract_data_from_database3(config)
upload_results(config,results)




