from datetime import datetime, timedelta
import time
from typing import Tuple, Optional

from diskcache import Cache
from geopy.geocoders.base import Geocoder
from geopy.geocoders import Nominatim

class CachedGeolocator:

    # TODO refactor to contex manager

    TIMEOUT = timedelta(seconds=1)
    next_call_at = datetime.utcnow() # thread unsafe 

    def __init__(self, 
            geocoder: Nominatim = None, 
            cache_dir: str = None, 
            antithrottling: bool = True, 
            verbose:bool=False):
        
        self.geocoder: Nominatim = geocoder or Nominatim(user_agent="transparent_chelicks")
        cache_dir = cache_dir or './'
        self.cache: Cache = Cache(directory=cache_dir)
        self.antithrottling = antithrottling
        self.verbose = verbose

    def __call__(self, query: str) -> Optional[Tuple[float, float]]:
        if query in self.cache:
            if self.verbose:
                print(f'+++In cache {query}')
            return self.cache[query]
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
            
            if point is not None:
                self.cache.set(query, point)

            return point
    
    def close(self):
        self.cache.close()

def location_fixer(location: str) -> str:
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

if __name__ == '__main__':
    assert location_fixer('Локачинський район, село Старий Загорів') == 'старий загорів'
    assert location_fixer('смт.Локачі') == 'локачі'

    geolocator = CachedGeolocator(cache_dir='./data/geo_cache', verbose=True)
    time.sleep(2)
    assert geolocator('оброшино львівська область') is not None
    assert geolocator('львів львівська область') is not None
    assert geolocator('оброшино львівська область') is not None
    assert geolocator('абабагаламага бармалей') is None