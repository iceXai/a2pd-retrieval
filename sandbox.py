
# In[]
from job import RetrievalJob

# In[]

job = RetrievalJob('agrs')
job.validate()
#job.setup()
#job.run()


#print(job.cfg.get_sensor())
#print(job.cfg.get_carrier())
#print(job.cfg.get_token())

#print(job.cfg.config)

#test = job.lst._get_date_strings()


# geometa_file_name = f'MOD03_2022-09-01.txt'
# url = f'https://ladsweb.modaps.eosdis.nasa.gov/archive/geoMeta/61/TERRA/2022/{geometa_file_name}'
# token = job.cfg.get_token()
# import requests
# r = requests.get(url,headers={'Authorization': "Bearer {}".format(token)})
# print(r.content)


from abc import ABC, abstractmethod
class A(ABC):
    @abstractmethod
    def test(self, string = 'test'):
        print(string)
    
    @abstractmethod  
    def test2(self):
        return 2
        

class B(A):
    def test(self, string):
        super().test()
        super().test(string)
    def test2(self):
        return super().test2()

class C(A):
    def test(self, string):
        print(string)
    def test2(self):
        return 4
        
b = B()
b.test('blubb')
x = b.test2(); print(str(x))

c = C()
c.test('bla')
x = c.test2(); print(str(x))

