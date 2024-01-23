import apache_beam as beam
from datetime import datetime
from beam_options import get_pipeline_options

def add_timestamp(element):
    timestamp = datetime.utcnow().isoformat()
    return element, timestamp

def run():
    options = get_pipeline_options()
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | 'Create Sample Data' >> beam.Create(['apple', 'banana', 'orange'])
            | 'Add Timestamp' >> beam.Map(add_timestamp)
            | 'Format for Output' >> beam.Map(lambda elements: ','.join(elements))
            | 'Write to Text File' >> beam.io.WriteToText('gs://apachebeam_1/output_data')
        )

if __name__ == '__main__':
    run()
