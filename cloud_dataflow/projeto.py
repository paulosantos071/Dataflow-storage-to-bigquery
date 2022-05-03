import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = {
    'project': 'curso-dataflow-beam-347613' ,
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_location': 'gs://curso-apachebeam-dataflow/temp',
    'temp_location': 'gs://curso-apachebeam-dataflow/temp',
    'template_location': 'gs://curso-apachebeam-dataflow/template/storage_to_bigquery',
    'save_main_session' : True }

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)

serviceAccount = r'C:\Users\paulo.santos\Documents\CURSO DATA FLOW\Python-master\curso-dataflow-beam-347613-c998cb1e5f49.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= serviceAccount


def criar_dict(record):
    dict_ = {}
    dict_['name'] 
    dict_['company'] = record[1]
    dict_['pin'] = record[2]
    return(dict_)

table_schema = 'name:STRING, company:STRING, pin:INTEGER'
tabela = 'projeto_curso_dataflow.Curso_dataflow_projeto'

Tabela_Dados = (
    p1
    | "Importar Dados" >> beam.io.ReadFromText(r"gs://bucket_trigger_dataflow_storage_to_bigquery/dataset.csv", skip_header_lines = 2)
    | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
    | "Criar um dic" >> beam.Map(lambda record: criar_dict(record)) 
    | "Gravar no BigQuery" >> beam.io.WriteToBigQuery(
                              tabela,
                              schema=table_schema,
                              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                              custom_gcs_temp_location = 'gs://curso-apachebeam-dataflow/temp' )
)

p1.run()