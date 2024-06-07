SELECT   row_id                    
 ,id                        
 ,listing_url               
 ,name                      
 ,room_type                 
 ,minimum_nights            
 ,host_id                   
 ,price                     
 ,created_at                
 ,updated_at                
 ,created_ts                
 ,metadata$filename         
 ,metadata$file_row_number  
from     {{ source('source_name_goibibo','listing_stream')}}
where  METADATA$ISUPDATE = 'FALSE' and METADATA$ACTION = 'INSERT'