import os
from flask import Blueprint, request, jsonify
import azureml.dataprep as dprep
from config import parquet_directory, component_count_map, event_id_count_map

count_routes = Blueprint('count', __name__)

@count_routes.route('/count_logs', methods=['GET'])
def count_logs():
    try:
        component = request.args.get('Component')
        event_id = request.args.get('EventId')
        level = request.args.get('Level')

        # check the cache for most common queries
        if component and not event_id and not level:
            if component in component_count_map:
                print(f'component {component} found in cache')
                response = {'count': component_count_map[component]}
            else:
                print(f'component {component} not found in cache, reading from parquet file(s)')
                dataflow = dprep.read_parquet_file(os.path.join(parquet_directory, 'part-*'))
                count = dataflow.filter(dataflow['Component'] == component).keep_columns(['LineId']).row_count
                component_count_map[component] = count
                response = {'count': count}
            return jsonify(response), 200

        if event_id and not component and not level:
            if event_id in event_id_count_map:
                print(f'event_id {event_id} found in cache')
                response = {'count': event_id_count_map[event_id]}
            else:
                print(f'event_id {event_id} not found in cache, reading from parquet file(s)')
                dataflow = dprep.read_parquet_file(os.path.join(parquet_directory, 'part-*'))
                count = dataflow.filter(dataflow['EventId'] == event_id).keep_columns(['LineId']).row_count
                event_id_count_map[event_id] = count
                response = {'count': count}
            return jsonify(response), 200

        # not in cache or uncommon query, so read from persisted parquet files
        print(f'Uncommon query or component of query not found in cache, reading from parquet file(s)')
        dataflow = dprep.read_parquet_file(os.path.join(parquet_directory, 'part-*'))
        if component:
            dataflow = dataflow.filter(dataflow['Component'] == component)
        if event_id:
            dataflow = dataflow.filter(dataflow['EventId'] == event_id)
        if level:
            dataflow = dataflow.filter(dataflow['Level'] == level)

        count = dataflow.keep_columns('LineId').row_count
        response = {'count': count}
        return jsonify(response), 200

    except Exception as e:
        print(e)
        if 'NotFound' in str(e):
            response = {'count': 0}
            return jsonify(response), 200
        error_message = f"Error: {str(e)}"
        return jsonify({'error': error_message}), 500