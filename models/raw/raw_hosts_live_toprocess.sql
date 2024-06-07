SELECT  row_id                         
     ,id                            
     ,name                          
     ,is_superhost                  
     ,created_at                    
     ,updated_at                    
     ,created_ts                    
     ,metadata$filename             
     ,metadata$file_row_number
from     {{ source('source_name_goibibo','hosts_stream')}}
where  METADATA$ISUPDATE = 'FALSE' and METADATA$ACTION = 'INSERT'