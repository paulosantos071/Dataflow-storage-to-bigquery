import apache_beam as beam

p1 = beam.Pipeline()

# def criar_dict(record):
#     dict_ = {}
#     dict_['name'] = record['name']
#     dict_['company'] = record['company']
#     dict_['pin'] = record['pin']  
#     return(dict_)

voos = (
p1
    | "Importar Dados" >> beam.io.ReadAllFromText("C:\Users\paulo.santos\Documents\CURSO DATA FLOW\Projetinho_paygo\dataset.csv", skip_header_lines = 1)
    | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
    | "Mostrar Resultados" >> beam.Map(print)
)

p1.run()