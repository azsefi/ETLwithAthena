class EventLoader:
    def __init__(self, source, out, access, secret):
        # output bucket to write result files
        self.output_folder = out
        # input bucket. s3://olx-bdt-recruit/de/horizontals/ for this case
        # becase the user provided doesn't have access to run athena
        # all files under horizontals copied into s3://olxdata/ to use my IAM user
        self.source_bucket = 's3://'+ source +'/'
        self.output_bucket = 's3://'+ out +'/'
        self.access = access
        self.secret = secret
        self.pathToEventDetails = None

        # athena client to run queries via athena
        self.athena = boto3.client('athena', region_name='us-west-2', 
                              aws_access_key_id = access, aws_secret_access_key = secret)

        # s3 connection
        self.s3 = boto3.resource('s3',region_name='us-west-2', 
                              aws_access_key_id = access, aws_secret_access_key = secret)

        # bucket object of output data
        self.bucket = self.s3.Bucket(self.output_folder)

    def clear(self, loc):
        '''
            Deletes files from the input path
            @params:
                loc: S3 path to be cleared
        '''
        objects_to_delete = []
        for obj in self.bucket.objects.filter(Prefix=loc):
            objects_to_delete.append({'Key': obj.key})

        self.bucket.delete_objects(
            Delete={
                'Objects': objects_to_delete
            }
        )


    def execute_query(self, query, loc=''):
        '''
            Executes input query via aws athena.
            @params:
                query: query text to execute
                loc  : output path of the query
        '''
        if loc != '':
            try:
                # Clear output path to prevent duplicates
                self.clear(loc)
            except:
                # If output path not exists
                raise

        response = self.athena.start_query_execution(
                                                QueryString=query,
                                                QueryExecutionContext={'Database': 'default'},
                                                ResultConfiguration={'OutputLocation': self.output_bucket+loc}
                                                )
        # Wait until result files of query become ready
        stat = self.waitexecution(response['QueryExecutionId'])
        return stat

    def waitexecution(self, queryid):
        '''
            A workaround to control sequential runnning of steps.
            Because, output folder cleared each time before an execution executed, 
            after query execution request this procedure starts to check if 
            .csv or .txt files generated. 
            If not generated: wait 5seconds, check again
            If generated: finish execution
        '''
        while True:
            stat = self.athena.get_query_execution(QueryExecutionId=queryid)['QueryExecution']
            if stat['Status']['State'] != 'RUNNING':
                return stat
            else:
                time.sleep(2)

    def add_partition(self, table_name, **kwargs):
        '''
            Probably data lake keeps historical logs, so it is
            more efficient and cost effective to add partition to events table and 
            select hydra events by partition, to parse files only from required day. 
            Creates partition based on input parameters.
            @params:
                key-value: config parameters
        '''
        partition_path  = '/'.join(kwargs.values())
        partition_query = ','.join([x+"='" + kwargs[x] + "'" for x in kwargs])

        query = "alter table " + table_name + " add if not exists partition ("+partition_query+") " +\
                "location '" + self.source_bucket+ partition_path + "'"

        self.execute_query(query, 'tables/')
        print ('Partition added to ' + table_name.upper() + ' table')

    def get_events(self, **kwargs):
        '''
            Reads hydra events (json files) and extracts country, session, date info
            @params:
                key_value: config params
        '''
        # config variable converted to SQL-where clause format
        query_filter = ' and '.join([x+"='" + kwargs[x] + "'" for x in kwargs])
        # path for output based on config params
        result_path  = 'events/' + '/'.join([x+"=" + kwargs[x] for x in kwargs]) + '/'

        query = '''select  meta.session_long as device_id,
                           meta.date as event_time,
                           params.cc as country
                 from events
                 where ''' + query_filter

        response = self.execute_query(query, result_path)
        return response

    def get_event_details(self,**kwargs):
        '''
            Reads hydra events (json files) and extracts country, session, date, 
            and location info
            @params:
                key_value: config params
        '''
        # config variable converted to SQL-where clause format
        query_filter = ' and '.join([x+"='" + kwargs[x] + "'" for x in kwargs])
        # path for output based on config params
        result_path  = 'event_details/' + '/'.join([x+"=" + kwargs[x] for x in kwargs]) + '/'

        query = '''select  meta.session_long as device_id,
                           meta.date as event_time,
                           params.cc as country,
                           params.long as longitude,
                           params.lat as latitude,
                           params.user_id as user_id
                 from event_details
                 where ''' + query_filter

        response = self.execute_query(query, result_path) 
        return response

    def refresh_partitions(self):
        '''
            Refreshs table structure, detects new partitions.
            Structure of source files of events_formatted table makes it
            possible to catch partitions automatically. But, to be able to
            query newly created source files table structure must be refreshed.
        '''
        query='MSCK REPAIR TABLE events_formatted'
        self.execute_query(query, 'tables/')
        print ('EVENTS_FORMATTED table partitions refreshed')

    def get_dau(self,**kwargs):
        ### Returns daily active user counts by country for given day
        query_filter = ' and '.join([x+"='" + kwargs[x] + "'" for x in kwargs])
        result_path  = 'dau/' + '/'.join([x+"=" + kwargs[x] for x in kwargs]) + '/'

        query = '''select * 
                 from   (select country,
                                count(distinct device_id) as customer_count
                           from events_formatted
                          WHERE device_id != 'device_id' and ''' + query_filter + \
                       ''' group by country)
                 where country is not null'''

        response = self.execute_query(query, result_path)
        return response

    def initialize_tables(self):
        ### Create events table to keep(read) extracted country, session, date 
        ### information from input json files. 
        query = '''CREATE external TABLE if not exists events (
                                                   meta struct<session_long: string,
                                                               date        : string>,
                                                   params struct<cc: string>
                                                )
        partitioned by (brand string, tracker string, channel string, year string, month string, day string)
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        LOCATION ''' + "'" + self.source_bucket + "'"

        response = self.execute_query(query, 'tables/')

        ### Create events_formatted table to keep(read) country, session, date information
        ### from events table in csv format. 
        query = '''CREATE EXTERNAL TABLE if not exists default.events_formatted (  device_id string,
                                   event_time string,
                                      country string )                             
        partitioned by (brand string, tracker string, channel string, year string, month string, day string)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'                               
        LOCATION ''' + "'" + self.output_bucket + "events/'"

        self.execute_query(query, 'tables/')

        query = '''CREATE external TABLE if not exists event_details (meta struct<session_long: string,
                                                                   date        : timestamp>,
                                                       params struct<user_id   : string, 
                                                                     cc        : string,
                                                                     long      : string,
                                                                     lat       : string,
                                                                     extra     : struct<user_type: string,
                                                                                        language : string>>
                                                    )
        partitioned by (brand string, tracker string, channel string, year string, month string, day string)
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        LOCATION '''+ "'" + self.source_bucket + "'"

        self.execute_query(query, 'tables/')
        
        print ('Tables initialized')

    def get_query_id(self):
        query_ids = self.athena.list_query_executions()['QueryExecutionIds']
        return query_ids[0]

    def readEventDetails(self, nrows):
        s3_client = boto3.client('s3', region_name='us-west-2', 
                              aws_access_key_id = access, aws_secret_access_key = secret)

        obj = s3_client.get_object(Bucket=self.output_folder, 
                                   Key=self.pathToEventDetails)

        self.event_details = pd.read_csv(obj['Body'], nrows=nrows)
        
    def load(self, **config):
        # create required table structures
        self.initialize_tables()
        
        # add partition to events table to query it efficiently
        self.add_partition('events', **config)
        
        # Get events data (country, session, date)
        response = self.get_events(**config)
        
        if (response['Status'].get('State') != 'SUCCEEDED'):
            print (response['Status'].get('StateChangeReason'))
            return -1
        else:
            print ('Data extracted from HYDRA events:')
            print ('\tExecutionTme:{0}s DataScanned:{1} mb'.format(response['Statistics'].get('EngineExecutionTimeInMillis')/1000,
                                                                   round((response['Statistics'].get('DataScannedInBytes')/1024/1024), 3)))
        
        # Refresh events_formatted table to detect new partition
        self.refresh_partitions()
        
        # Get DAU information
        response = self.get_dau(**config)
        if (response['Status'].get('State') != 'SUCCEEDED'):
            print (response['Status'].get('StateChangeReason'))
            return -1
        else:
            print ('DAU data extracted:')
            print ('\tExecutionTme:{0}s DataScanned:{1} mb'.format(response['Statistics'].get('EngineExecutionTimeInMillis')/1000,
                                                                   round((response['Statistics'].get('DataScannedInBytes')/1024/1024), 3)))
        
        self.add_partition('event_details', **config)
        
        response = self.get_event_details(**config)
        if (response['Status'].get('State') != 'SUCCEEDED'):
            print (response['Status'].get('StateChangeReason'))
            return -1
        else:
            print ('Event datails extracted:')
            print ('\tExecutionTme:{0}s DataScanned:{1} mb'.format(response['Statistics'].get('EngineExecutionTimeInMillis')/1000,
                                                                   round((response['Statistics'].get('DataScannedInBytes')/1024/1024), 3)))
        
        self.pathToEventDetails = response['ResultConfiguration'].get('OutputLocation')
        self.pathToEventDetails = self.pathToEventDetails.replace(self.output_bucket, '')
        
    def plot_country(self, country, country_borders, city):
        events = self.event_details[self.event_details.country==country]
        plt.figure(figsize=(18,10))    
        mymap = Basemap(projection='merc',
            resolution = 'l', area_thresh = 0.1,
            llcrnrlon=country_borders[0][0], llcrnrlat=country_borders[0][1],
            urcrnrlon=country_borders[1][0], urcrnrlat=country_borders[1][1])

        mymap.drawcoastlines()
        mymap.drawcountries()
        mymap.fillcontinents()
        mymap.drawmapboundary()

        lons = events.longitude.values
        lats = events.latitude.values
        x,y = mymap(lons, lats)
        mymap.plot(x, y, 'ro', markersize=0.5)

        x,y = mymap(city['longs'], city['lats'])
        mymap.plot(x, y, 'bo', markersize=5)

        for label, xpt, ypt in zip(city['labels'], x, y):
            plt.text(xpt+10000, ypt+10000, label)
            
        plt.show()