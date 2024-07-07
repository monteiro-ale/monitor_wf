import json
from random import uniform
import time
from datetime import datetime

# Contador para controlar as rodadas
contador = 0

while True:
    registros = []
    for id_turbina in range(1, 11):
        dados_pf = uniform(0.7, 1)
        dados_hp = uniform(70, 80)
        
        if contador < 2:
            # Duas rodadas com temperatura abaixo de 24°C
            dados_tp = uniform(20, 23.9)
        else:
            # Uma rodada com temperatura aleatória
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


    contador = (contador + 1) % 3

    time.sleep(5)




# import json
# from random import uniform
# import time
# from datetime import datetime

# while True:
# 		registros = []
# 		for id_turbina in range(1, 11):
# 				dados_pf = uniform(0.7, 1)
# 				dados_hp = uniform(70, 80)
# 				dados_tp = uniform(20, 25)

# 				registro = {
# 						'turbine_id': str(id_turbina),
# 						'powerfactor': str(dados_pf),
# 						'hydraulicpressure': str(dados_hp),
# 						'temperature': str(dados_tp),
# 						'timestamp': str(datetime.now())
# 				}

# 				registros.append(registro)
		
# 		with open('C:/Users/usuario/Desktop/monitor_wf/airflow/data/data.json', 'w') as fp:
# 				json.dump(registros, fp)

# 		time.sleep(5)
