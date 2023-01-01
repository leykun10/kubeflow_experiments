
import kfp.dsl as dsl
import kfp
from kfp.components import create_component_from_func,InputPath, OutputPath


client = kfp.Client('http://localhost:8080')



def get_data(output_path:OutputPath(str)):
    import pandas as pd
    import io
    import requests
    url="https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv"
    s=requests.get(url).content
    c=pd.read_csv(io.StringIO(s.decode('utf-8')))
    c.to_csv(output_path)


get_data_task = create_component_from_func(get_data,base_image='amancevice/pandas:x.y.z-alpine')


def transform_data(input_path=InputPath(str)):
    import pandas as pd
    data=pd.read_csv(input_path)
    tfm_data=data.groupby(by=['Region'])['Country'].apply(list).reset_index()
    print(tfm_data)


transform_data_task = create_component_from_func(transform_data)


import kfp.dsl as dsl
@dsl.pipeline(
  name='ETL pipeline',
  description='an etl pipeline that extract data from the internet,loads data and transform data.'
)
def etl_pipeline():
   first_task = get_data_task() 
   second_task = transform_data_task(input_path=first_task.outputs['output'])
arguements={}
client.create_run_from_pipeline_func(etl_pipeline,arguments=arguements)