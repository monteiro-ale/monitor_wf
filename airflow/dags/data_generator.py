import json
from random import uniform
import time
from datetime import datetime

while True:
		registros = []
		for id_turbina in range(1, 11):
				dados_pf = uniform(0.7, 1)
				dados_hp = uniform(70, 80)
				dados_tp = uniform(20, 25)

				registro = {
						'turbine_id': str(id_turbina),
						'powerfactor': str(dados_pf),
						'hydraulicpressure': str(dados_hp),
						'temperature': str(dados_tp),
						'timestamp': str(datetime.now())
				}

				registros.append(registro)
		
		with open('C:/Users/usuario/Desktop/monitor_wf/airflow/data/data.json', 'w') as fp:
				json.dump(registros, fp)

		time.sleep(5)
