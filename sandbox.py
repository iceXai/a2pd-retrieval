
# In[]
from job import RetrievalJob

# In[]

job = RetrievalJob('agrs')
job.validate()
job.setup()
job.run()


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



import pandas as pd

d = {'a': [1,2], 'b': [2,3], 'c': ['bla','blubb']}
df = pd.DataFrame(d)

for i,e in df.iterrows():
    print(e)

# from abc import ABC, abstractmethod
# class A(ABC):
#     @abstractmethod
#     def test(self, string = 'test'):
#         print(string)
    
#     @abstractmethod  
#     def test2(self):
#         return 2
        

# class B(A):
#     def test(self, string):
#         super().test()
#         super().test(string)
#     def test2(self):
#         return super().test2()

# class C(A):
#     def test(self, string):
#         print(string)
#     def test2(self):
#         return 4
        
# b = B()
# b.test('blubb')
# x = b.test2(); print(str(x))

# c = C()
# c.test('bla')
# x = c.test2(); print(str(x))

# print('----')

# class Z(ABC):
#     def __init__(self):
#         self.a = '1'
#         self.z = self.z(self)
        
#     def init_X(self):
#         self.x = X(self)
        
#     def show(self):
#         print(self.a)
        
#     def show_z(self):
#         self.z.show()
    
#     class z(object):
#         def __init__(self, outer_self):
#             self.outer_self = outer_self
#             self.a = '2'
#         def show(self):
#             print(self.a)
            
#         def show_Z(self):
#             self.outer_self.show()
            
# class X(object):
#     def __init__(self, other_self):
#         self.other_self = other_self
#         self.other_self.show()
        
#     def set_outer_a(self):
#         self.other_self.a = '3'
#         self.other_self.show()


# test = Z()
# test.show()
# test.show_z()
# test.z.show_Z()
# print('----')
# test.init_X()
# test.x.set_outer_a()
# test.show()