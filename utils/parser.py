from utils.text_helper import sanitize_path


def generate_inputs(parsed_yml_file, team_filter: list, delta_dependency_specs: dict, stage_dependency_specs: dict,
                    processed_dependency_specs: dict, raw_dependency_specs: dict):
    for project_folder, project_dags in parsed_yml_file.get("spark_jobs").items():
        for dag_name, dag_info in project_dags.items():
            if dag_info.get('team') in team_filter:
                for tasks, tasks_info in dag_info.items():
                    if tasks == "tasks":
                        for task in tasks_info:
                            for task_key, task_value in task.items():
                                if task_key in ["output", "input"]:
                                    for output_specs in task_value:
                                        for type, path in output_specs.items():
                                            if type == "name":
                                                if sanitize_path(path)[0:5] == 'delta':
                                                    delta_dependency_specs.setdefault(sanitize_path(path), [])
                                                if sanitize_path(path)[0:5] == 'stage':
                                                    stage_dependency_specs.setdefault(sanitize_path(path), [])
                                                if sanitize_path(path)[0:9] == 'processed':
                                                    processed_dependency_specs.setdefault(sanitize_path(path), [])
                                                if sanitize_path(path)[0:3] == 'raw':
                                                    raw_dependency_specs.setdefault(sanitize_path(path), [])

    return delta_dependency_specs, stage_dependency_specs, processed_dependency_specs, raw_dependency_specs


def link_inputs(parsed_yml_file, team_filter: list, delta_dependency_specs: dict, stage_dependency_specs: dict,
                processed_dependency_specs: dict, raw_dependency_specs: dict):
    for project_folder, project_dags in parsed_yml_file.get("spark_jobs").items():
        for dag_name, dag_info in project_dags.items():
            if dag_info.get('team') in team_filter:
                for tasks, tasks_info in dag_info.items():
                    if tasks == "tasks":
                        for task in tasks_info:
                            for task_key, task_value in task.items():
                                if task_key in ["output", "input"]:
                                    for input_specs in task_value:
                                        for type, path in input_specs.items():
                                            if type == "name":
                                                if sanitize_path(path) in delta_dependency_specs.keys():
                                                    if dag_name not in delta_dependency_specs[sanitize_path(path)]:
                                                        delta_dependency_specs[sanitize_path(path)].append(dag_name)
                                                if sanitize_path(path) in stage_dependency_specs.keys():
                                                    if dag_name not in stage_dependency_specs[sanitize_path(path)]:
                                                        stage_dependency_specs[sanitize_path(path)].append(dag_name)
                                                if sanitize_path(path) in processed_dependency_specs.keys():
                                                    if dag_name not in processed_dependency_specs[sanitize_path(path)]:
                                                        processed_dependency_specs[sanitize_path(path)].append(dag_name)
                                                if sanitize_path(path) in raw_dependency_specs.keys():
                                                    if dag_name not in raw_dependency_specs[sanitize_path(path)]:
                                                        raw_dependency_specs[sanitize_path(path)].append(dag_name)

    return delta_dependency_specs, stage_dependency_specs, processed_dependency_specs, raw_dependency_specs
