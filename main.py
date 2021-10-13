import yaml

from utils import parser

if __name__ == '__main__':
    yml_file = open("spark")
    spark_yml_file = yaml.load(yml_file, Loader=yaml.FullLoader)

    delta_dependency_specs = {}
    stage_dependency_specs = {}
    processed_dependency_specs = {}
    raw_dependency_specs = {}

    delta_dependency_specs, stage_dependency_specs, processed_dependency_specs, raw_dependency_specs = parser.generate_inputs(
        parsed_yml_file=spark_yml_file,
        team_filter=['{{ airflow_dag_owners.conn_cc.name }}'],
        delta_dependency_specs=delta_dependency_specs,
        raw_dependency_specs=raw_dependency_specs,
        processed_dependency_specs=processed_dependency_specs,
        stage_dependency_specs=stage_dependency_specs)

    delta_dependency_specs, stage_dependency_specs, processed_dependency_specs, raw_dependency_specs = parser.link_inputs(
        parsed_yml_file=spark_yml_file,
        team_filter=['{{ airflow_dag_owners.conn_cc.name }}'],
        delta_dependency_specs=delta_dependency_specs,
        raw_dependency_specs=raw_dependency_specs,
        processed_dependency_specs=processed_dependency_specs,
        stage_dependency_specs=stage_dependency_specs)

    with open(r'delta_dependency_specs.yaml', 'w') as file:
        yaml.dump(delta_dependency_specs, file)
    with open(r'stage_dependency_specs.yaml', 'w') as file:
        yaml.dump(stage_dependency_specs, file)
    with open(r'processed_dependency_specs.yaml', 'w') as file:
        yaml.dump(processed_dependency_specs, file)
    with open(r'raw_dependency_specs.yaml', 'w') as file:
        yaml.dump(raw_dependency_specs, file)
