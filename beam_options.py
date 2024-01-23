from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions

def get_pipeline_options():
    # Replace 'your-project' with your project ID and 'your-bucket-name' with your bucket name.
    pipeline_options = {
        'project': 'devops-411809',
        'staging_location': 'gs://apachebeam_1/staging',
        'temp_location': 'gs://apachebeam_1/temp',
        'runner': 'DataflowRunner',
        'region': 'europe-west1',  # Update this to your desired region
        'setup_file': './setup.py',
    }

    options = PipelineOptions.from_dictionary(pipeline_options)
    options.view_as(SetupOptions).save_main_session = True
    return options
