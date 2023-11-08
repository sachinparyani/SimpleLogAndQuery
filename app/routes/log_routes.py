import os
from flask import Blueprint, request, jsonify
import azureml.dataprep as dprep
from azureml.dataprep.api.functions import get_portable_path
import threading
import uuid
from config import log_directory, parquet_directory, component_count_map, event_id_count_map

log_routes = Blueprint('log', __name__)

@log_routes.route('/write_logs', methods=['POST'])
def write_logs():
    try:
        # Get the request data (CSV log content)
        log_data = request.data.decode('utf-8')

        # Generate a unique log filename or use a timestamp-based name
        log_filename = os.path.join(log_directory, f"log_{len(os.listdir(log_directory))}.csv")

        # Write the log data to a file
        with open(log_filename, 'w') as log_file:
            log_file.write(log_data)

        def to_parquet_and_cache_update(file):
            # use dprep to convert csv to parquet
            try:
                id = str(uuid.uuid4())
                dflow = dprep.read_csv(file).to_parquet_streams().add_column(get_portable_path(dprep.col('Path')) + id, 'filenames', 'Path')
                dflow.write_streams(streams_column='Path',
                                    base_path=dprep.LocalFileOutput(f'{parquet_directory}/'),
                                    file_names_column='filenames',
                                    prefix_path='/').run_local()
                # delete csv file after conversion
                os.remove(file)

                # update the cache by reading the parquet file and aggregating the counts for each component and event id
                dataflow = dprep.read_parquet_file(os.path.join(parquet_directory, f'*{id}')).drop_nulls('LineId')
                dataflow_component_update = dataflow.summarize(
                    summary_columns=[
                        dprep.SummaryColumnsValue(
                            column_id='Component',
                            summary_column_name='ComponentCount',
                            summary_function=dprep.SummaryFunction.COUNT)],
                        group_by_columns=['Component'],)
                dataflow_event_id_update = dataflow.summarize(
                    summary_columns=[
                        dprep.SummaryColumnsValue(
                            column_id='EventId',
                            summary_column_name='EventIdCount',
                            summary_function=dprep.SummaryFunction.COUNT)],
                        group_by_columns=['EventId'],)

                # for each component in the dataflow, update the cache with the count by adding to the existing count
                for row in dataflow_component_update.to_pandas_dataframe().itertuples():
                    if row.Component in component_count_map:
                        component_count_map[row.Component] += row.ComponentCount
                    else:
                        component_count_map[row.Component] = row.ComponentCount

                # for each event_id in the dataflow, update the cache with the count by adding to the existing count
                for row in dataflow_event_id_update.to_pandas_dataframe().itertuples():
                    if row.EventId in event_id_count_map:
                        event_id_count_map[row.EventId] += row.EventIdCount
                    else:
                        event_id_count_map[row.EventId] = row.EventIdCount 

            except Exception as e:
                print(f"Error converting csv to parquet or while getting count: {str(e)}.")
                raise e 
                

        # spawn a background thread to convert csv to parquet
        threading.Thread(target=to_parquet_and_cache_update, args=(log_filename,)).start()

        # Respond with a 200 status code and success message
        response = {'message': 'Log data saved successfully'}
        return jsonify(response), 200

    except Exception as e:
        # Handle exceptions and respond with an error status code
        error_message = f"Error: {str(e)}"  
        return jsonify({'error': error_message}), 500