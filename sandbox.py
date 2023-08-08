
# In[]
from job import RetrievalJob

# In[]

job = RetrievalJob('agrs')
job.validate()
job.setup()


print(job.cfg.get_sensor())
print(job.cfg.get_carrier())
print(job.cfg.get_token())

print(job.cfg.config)

#test = job.lst._get_date_strings()

job.run()

# geometa_file_name = f'MOD03_2022-09-01.txt'
# url = f'https://ladsweb.modaps.eosdis.nasa.gov/archive/geoMeta/61/TERRA/2022/{geometa_file_name}'
# token = job.cfg.get_token()
# import requests
# r = requests.get(url,headers={'Authorization': "Bearer {}".format(token)})
# print(r.content)



