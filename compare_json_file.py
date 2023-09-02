from datetime import date, datetime
import json
import decimal
import sys
from time import sleep
from google.cloud import bigquery

# BigQuery Client
bq = bigquery.Client()

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, (decimal.Decimal)):
        return float(obj)
    else:
        return(str(obj))
    raise TypeError ("Type %s not serializable" % type(obj))
    sys.exit(1)

def load_json_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)

# Permet de requêter Bigquery
def query(sql):
    query_job = bq.query(sql)
    results = query_job.result()
    return(results)

def run_with_retry(func: callable, max_retries: int = 5, wait_seconds: int = 20, **func_params):
    num_retries = 1
    while True:
        try:
            return func(*func_params.values())
        except Exception as e:
            if num_retries > max_retries:
                print('we have reached maximum errors and raising the exception')
                raise e
            else:
                print(f'{num_retries}/{max_retries}')
                print("Retrying error:", e)
                num_retries += 1
                sleep(wait_seconds)

def collect_docs(project_lake, start_timestamp, end_timestamp):
    q_collect_all_docs_to_check='''
SELECT cd.product,
            cd.publishTime,
            od.publishTime as previous_publishTime,
            ARRAY_AGG(cd.content.data) as current_data,
            ARRAY_AGG(od.content.data IGNORE NULLS) as old_data
  FROM my_project.my_dataset.current_data cd
  LEFT JOIN my_project.my_dataset.old_data od ON cd.product = od.product
  GROUP BY 1, 2, 3
    '''
    
    print(q_collect_all_docs_to_check)
    docs_to_check = query(q_collect_all_docs_to_check)
    print("Fin de la collecte")
    return docs_to_check

def find_list_differences(key_list, list1, list2, path, diffs):
    for key in key_list:
        element_map1 = {item.get(key): item for item in list1}
        element_map2 = {item.get(key): item for item in list2}

        common_keys = set(element_map1.keys()) & set(element_map2.keys())

        for key in common_keys:
            new_path = f"{path}.{key}" if path else key
            find_differences(element_map1[key], element_map2[key], new_path, diffs)

        added_keys = set(element_map2.keys()) - set(element_map1.keys())
        for key in added_keys:
            new_path = f"{path}.{key}" if path else key
            diffs.append({"path": new_path, "operation": "added", "old_value": None, "new_value": str(element_map2[key])})

        removed_keys = set(element_map1.keys()) - set(element_map2.keys())
        for key in removed_keys:
            new_path = f"{path}.{key}" if path else key
            diffs.append({"path": new_path, "operation": "removed", "old_value": str(element_map1[key]), "new_value": None})

def find_differences(obj1, obj2, path, diffs=None):
    if diffs is None:
        diffs = []

    if obj1 != obj2:
        if isinstance(obj1, dict) and isinstance(obj2, dict):
            common_keys = set(obj1.keys()) & set(obj2.keys())
            for key in common_keys:
                new_path = f"{path}.{key}" if path else key
                find_differences(obj1[key], obj2[key], new_path, diffs)

            added_keys = set(obj2.keys()) - set(obj1.keys())
            for key in added_keys:
                new_path = f"{path}.{key}" if path else key
                diffs.append({"path": new_path, "operation": "added", "old_value": None, "new_value": str(obj2[key])})

            removed_keys = set(obj1.keys()) - set(obj2.keys())
            for key in removed_keys:
                new_path = f"{path}.{key}" if path else key
                diffs.append({"path": new_path, "operation": "removed", "old_value": str(obj1[key]), "new_value": None})

        elif isinstance(obj1, list) and isinstance(obj2, list) and len(obj1) == len(obj2):
            for i in range(len(obj1)):
                new_path = f"{path}[{i}]" if path else f"[{i}]"
                find_differences(obj1[i], obj2[i], new_path, diffs)

        else:
            diffs.append({"path": path, "operation": "modified", "old_value": str(obj1), "new_value": str(obj2)})

    return diffs

def compare_json_files(json1, json2, list_key_map):

    differences = []

    # Analyse des champs qui ne sont pas des listes et n'ont pas de clés spécifiées
    non_list_keys = set(json1.keys()) & set(json2.keys()) - set(list_key_map.keys())
    for key in non_list_keys:
        find_differences(json1[key], json2[key], key, differences)

    # Analyse des champs qui sont des listes
    for key in list_key_map:
        list1 = json1.get(key, [])
        list2 = json2.get(key, [])
        find_list_differences(list_key_map[key], list1, list2, key, differences)
    
    return differences

def send_updates_to_GCP(data, table_path):
    job_config = bigquery.LoadJobConfig(
        autodetect=True, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )
    job = bq.load_table_from_json(data, table_path, job_config = job_config)
    print(job.result())

if __name__ == "__main__":
        
    docs_to_check = run_with_retry(func=collect_docs)
        
    records = [dict(row) for row in docs_to_check]
    json_obj = json.loads(json.dumps(records, default=json_serial, ensure_ascii=False))

    all_updates=[]
    first_doc = {}
    second_doc = {}
    
    #Create a key file if needed
    keys_path = 'my_list_keys.json'
    
    list_key_map = load_json_file(keys_path)

    for row in json_obj:
        row_update={}
        
        row_update['product'] = row['product']
        row_update['publishTime'] = row['publishTime']
        row_update['updates'] = []

        #Ne pas comparer lorsqu'il n'y a pas de document précédent
        if row['old_data']:
            row_update['updates'] = compare_json_files(row['old_data'][0], row['current_data'][0], list_key_map)
            #On ajoute dans les résultats uniquement s'il y a une modification
            if row_update['updates']:
                all_updates.append(row_update)
    data_serialized = json.loads(json.dumps(all_updates, default=json_serial, ensure_ascii=False))

    print('Envoi des données dans la table de résultat')
    if data_serialized:
        run_with_retry(func=send_updates_to_GCP, param1=data_serialized, param2='my_project.my_dataset.my_destination_table')
        print("Nb de modifs détectées : ", len(data_serialized))
    else:
        print("pas de données aujourd'hui")