from datetime import datetime, timedelta
import time
from typing import Tuple, Optional

from diskcache import Cache
from geopy.geocoders import Nominatim


ALLOWED_COUNTRIES = ['україна']

class CachedGeolocator:

    # TODO refactor to contex manager

    TIMEOUT = timedelta(seconds=1)
    next_call_at = datetime.utcnow() # thread unsafe 

    def __init__(self, 
            geocoder: Nominatim = None, 
            cache_dir: str = None, 
            bad_cache_dir: str = None, 
            antithrottling: bool = True, 
            verbose:bool=False):
        
        self.geocoder: Nominatim = geocoder or Nominatim(user_agent="transparent_chelicks", timeout=10000)
        self.cache: Cache = Cache(directory=cache_dir) if cache_dir else None
        self.bad_cache: Cache = Cache(directory=bad_cache_dir) if bad_cache_dir else None
        self.antithrottling = antithrottling
        self.verbose = verbose

    def __call__(self, query: str) -> Optional[Tuple[float, float]]:
        if self.cache and query in self.cache:
            if self.verbose:
                print(f'+++In cache {query}')
            return self.cache[query]
        elif self.bad_cache and query in self.bad_cache:
            # not to wait on multiple runs
            if self.verbose:
                print(f'+++In BAD cache {query}')
            return None
        else:
            if self.verbose:
                print(f'---Call for {query}')
            
            # wait for timeout it antithrottling mode
            if self.antithrottling:
                if self.next_call_at>datetime.utcnow():
                    time.sleep((self.next_call_at-datetime.utcnow()).total_seconds())
                
                self.next_call_at = datetime.utcnow() + self.TIMEOUT

            geolocation = self.geocoder.geocode(query)
            point = (geolocation.point.latitude, geolocation.point.longitude) if geolocation else None
            if self.verbose and point is None:
                print(f'!!!Not found {query}')
            
            if point is None:
                # to make 
                if self.bad_cache:
                    self.bad_cache.set(query, point)
            else:
                if self.cache:
                    self.cache.set(query, point)

            return point
    
    def close(self):
        if self.cache:
            self.cache.close()
        if self.bad_cache:
            self.bad_cache.close()


def locality_fixer(location: str) -> Optional[str]:
    '''Ugly but efficiet way to fix human bugs '''

    location = location.lower().strip()
    # # latin to cyrylic
    # latin = 'abexctmkopi'
    # cyryl = 'авехстмкорі'
    # for pos, letter in enumerate(latin):
    #     location = location.replace(letter,cyryl[pos])

    location = location.replace(',',' ')
    location = location.replace('.',' ')
    location = location.replace('р-н','район')

    # clean loc titles
    for na in ['селище міського типу ', 'село міського типу ', 'смт ', 'селище ', 'село ', 'місто ']:
        location = location.replace(na,'')

    # to nomative
    location=location.replace('ого району', 'ий район').strip()

    # swap region and city if region is found
    middle_rn = location.find('район ')
    if middle_rn>=0:
        location = (location[middle_rn+6:]+' '+location[0:middle_rn+6])
    
    loc_list = location.split()

    # cut shortcuts
    loc_list = [loc for loc in loc_list if len(loc)>2]

    # no region!!!1 - works better
    if 'район' in loc_list:
        loc_list = loc_list[0:-2]

    return ' '.join(loc_list)

def to_lower_fixer(location: str) -> Optional[str]:
    return location.lower().strip()

def region_fixer(location: str) -> Optional[str]:
    query = location.lower().strip()
    # kyiv fix 
    return query.replace('м. ', '').replace('місто ', '')

def countryName_fixer(location: str) -> Optional[str]:
    query = location.lower().strip() 

    if query not in ALLOWED_COUNTRIES:
        return None

    return query

def address_to_location(address, geolocator, fixers=None):
    
    point = None
    if not address:
        return point

    apply_fixer = lambda data, field_name, fixers : fixers[field_name](data.get(field_name,''))  if (field_name in fixers) and isinstance(data.get(field_name), str) else data.get(field_name,'') 
    fixers = fixers or {}

    locality = apply_fixer(address, 'locality', fixers)
    region = apply_fixer(address, 'region', fixers)
    countryName = apply_fixer(address, 'countryName', fixers)
    
    query_info=[locality,region,countryName]

    # print(query_info)
    if all(query_info):
        point = geolocator(' '.join(query_info))

    # try to search by postal code
    if point is None and 'postalCode' in address:
        postalCode = apply_fixer(address, 'postalCode', fixers)
        query_info=[postalCode,region,countryName]
        if all(query_info):
            point = geolocator(' '.join(query_info))

    # if point is None:
    #     print('Not located', query_info)

    return point

if __name__ == '__main__':
    assert locality_fixer('Локачинський район, село Старий Загорів') == 'старий загорів'
    assert locality_fixer('смт.Локачі') == 'локачі'

    geolocator = CachedGeolocator(cache_dir='./data/geo_cache', verbose=True)
    time.sleep(2)
    assert geolocator('оброшино львівська область') is not None
    assert geolocator('львів львівська область') is not None
    assert geolocator('оброшино львівська область') is not None
    assert geolocator('абабагаламага бармалей') is None