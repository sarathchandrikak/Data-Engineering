select 
locationid, 
borough,
zone,
replace(service_zone, 'Boro', 'Green') as serice_zone
from {{ref('taxi_zone_lookup')}}