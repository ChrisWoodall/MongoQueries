db.jobqueue.aggregate(

	[
		{
			'$match': {
			    'jobstartat': {"$lt": end, "$gt": start},
			    'jobstatus': "DONE"
			}
		},
		{
			'$project': {
			  'jobid': 1,
			  'clientid': 1, 
			  'brandid': 1, 
			  'jobworktickets': 1,
			  'jobtype': 1,
			  'spotfile_values': "$jobresult.auditts.values", 
			  'spot_start': {'$min': "$jobresult.auditts.date"},
			  'spot_finish': {'$max': "$jobresult.auditts.date"},
			  'granularity': {'$arrayElemAt': ["$jobresult.auditts.gran", 0]},
			  'jobstartat': 1,
			  'jobstarted': 1,
			  'jobfinished': 1,
			  'jobduration': { '$divide': [{"$subtract": ["$jobfinished", "$jobstarted"]}, 1000]},
			  'jobwaiting': { '$divide': [{"$subtract": ["$jobstarted", "$jobstartat"]}, 1000]},
			  'jobtotal': { '$divide': [{"$subtract": ["$jobfinished", "$jobstartat" ]}, 1000]}
			}
		},
		{
			'$unwind': {
			    'path' : "$spotfile_values",
			    
			    'preserveNullAndEmptyArrays' : True 
			}
		},
		{
			'$unwind': {
			    'path' : "$spotfile_values",
			    'preserveNullAndEmptyArrays' : True 			}
		},
		{
			'$group': {
			      '_id': {
			            'clientid': "$clientid", 
			            'brandid': "$brandid", 
			            'workticketid': "$jobworktickets", 
			            'spot_start': "$spot_start", 
			            'spot_finish': "$spot_finish", 
			            'granularity': "$granularity", 
			            'spot_duration': { '$divide': [{"$subtract": ["$spot_finish", "$spot_start"]}, 1000]},
			            'jobtype': "$jobtype",
			            'jobstartat': "$jobstartat",
			            'jobstarted': "$jobstarted",
			            'jobfinished': "$jobfinished",
			            'jobduration': "$jobduration",
			            'jobwaiting': "$jobwaiting",
			            'jobtotal': "$jobtotal", 
			              },
			      'spot_total': {'$sum': "$spotfile_values"},
			     
			}
		},
		{
			'$project': {
			      '_id':False,
			    'clientid': "$_id.clientid",
			    'brandid': "$_id.brandid",
			    'workticketid': "$_id.workticketid", 
			    'jobtype': "$_id.jobtype",
			    'jobstartat': "$_id.jobstartat",
			    'jobstarted': "$_id.jobstarted",
			    'jobfinished': "$_id.jobfinished",
			    'jobduration': "$_id.jobduration",
			    'jobwaiting': "$_id.jobwaiting",
			    'jobtotal': "$_id.jobtotal", 
			    'spot_start': "$_id.spot_start",
			    'spot_finish': "$_id.spot_finish",
			    'spot_duration': "$_id.spot_duration",
			    'granularity': "$_id.granularity",
			    'spot_total': 1,
			
			
			}
		},
		{
			'$sort': {
			    'jobstarted': 1
			}
		},
		{
			'$unwind': {
			    'path' : "$workticketid",
			    'preserveNullAndEmptyArrays': False
			}
		},
		{
			'$group': {
			'_id': {'clientid': "$clientid", 
			      'brandid': "$brandid", 
			      'workticketid': "$workticketid", 
			      },
				'jobcount': {"$sum": 1},
				'workticket_scheduled': {"$min": "$jobstartat"},
				'workticket_started': {"$min": "$jobstarted"},
				'workticket_finished': {"$max": "$jobfinished"},
				'duration': {"$sum": "$jobduration"},
				'waiting': {"$sum": "$jobwaiting"},
				'total': {"$sum": "$jobtotal"},
				'jobs': {"$push":  "$jobtype"},
				'spot_start': {"$min": "$spot_start" },
				'spot_finish': {"$max": "$spot_finish"},
				'spot_duration': {"$max": "$spot_duration"},
				'spot_total': {"$max": "$spot_total" },
				'granularity': {"$addToSet": "$granularity"}
			}		
		},
		{
			'$sort': {
			    'granularity': 1,
			}
		},
		{
			'$project': {
			    '_id': false,
			    'workticketid': "$_id.workticketid",
			    'clientid': "$_id.clientid",
			    'brandid': "$_id.brandid",
			    'workticket_scheduled': "$workticket_scheduled",
			    'workticket_started': "$workticket_started", 
			    'workticket_finished': "$workticket_finished",
			    'workticket_duration': { '$divide': [{"$subtract": ["$workticket_finished", "$workticket_scheduled"]}, 1000]},
			    'jobcount': "$jobcount",
			    'jobs': 1,
			    'total_time_under_calculation': "$duration",
			    'total_time_waiting': "$waiting",
			    'total_time_in_queue': "$total",   
			    'first_job': {'$arrayElemAt': ["$jobs", 0]},
			    'last_job': {'$arrayElemAt': ["$jobs", -1]},
			    'second_last_job': {'$arrayElemAt': ["$jobs", -2]},
			    'spot_start': "$spot_start",
			    'spot_finish': "$spot_finish",
			    'spot_granularity_0': {'$arrayElemAt': ["$granularity", 0]},
			    'spot_granularity_1': {'$arrayElemAt': ["$granularity", 1]},
			    'spot_duration': "$spot_duration",
			    'spot_total': "$spot_total"
			}
		},
		{
			'$match': {
			   'jobs': "LOAD.SPOTS",
			   'jobcount': {'$gte': 6},
			   '$or': [ 
			           {"last_job": { '$regex': "INF.SENDEMAIL"}}, 
			           {"last_job": { '$regex': "NOTIFY."}}, 
			            {"last_job": { '$regex': "PUBLISH."}},
			           {'$and': [ {"last_job": "INF.SCRUBDB"},{"second_last_job": {'regex': "NOTIFY."}}]},
			           {'$and': [ {"last_job": "INF.SCRUBDB"},{"second_last_job": "INF.SENDEMAIL"}]},
			           {'$and': [ {"last_job": "INF.SCRUBDB"},{"second_last_job": {'regex': "PUBLISH."}}]},
			           ],
			}
		},
		{
			'$project': {
			    '_id': False,
			    'workticketid': 1,
			    'clientid': 1,
			    'brandid': 1,
			    'workticket_scheduled': 1,
			    'workticket_started': 1, 
			    'workticket_finished': 1,
			    'workticket_duration': 1,
			    'jobcount': 1,
			    'jobs': 1,
			    'total_time_under_calculation': 1,
			    'total_time_waiting': 1,
			    'total_time_in_queue': 1,   
			    'first_job': 1,
			    'last_job': 1,
			    'second_last_job': 1,
			    'spot_start': 1,
			    'spot_finish': 1,
			    'spot_duration': 1,
			    'spot_granularity_1': 1,
			    'spot_total': 1
			}
		},
	],

);
