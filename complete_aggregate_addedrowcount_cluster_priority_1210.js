db.jobqueue.aggregate(

	// Pipeline
	[
		// Stage 1
		{
			$match: {
			    jobstartat: {"$lt": ISODate('2017-09-19'), "$gt": ISODate('2017-09-09')},
			    jobstatus: "DONE",
			    //jobtype: "MODEL.SPOT"
			}
		},

		// Stage 2
		{
			$project: {
			  jobid: 1,
			  clientid: 1, 
			  brandid: 1, 
			  jobworktickets: 1,
			  jobtype: 1, 
			  cluster_info: "$inf.pgcluster",
			  jobpriority:  "$jobpriority",
			  total_cost: "$jobresult.totalcost",
			  totalrowcount: "$jobresult.totalrowcount",
			  modelrowcount: "$jobresult.modelrowcount",
			  spotfile_cost_and_values: "$jobresult.auditts",
			  spot_start: {'$min': "$jobresult.auditts.date"},
			  spot_finish: {'$max': "$jobresult.auditts.date"},
			  granularity: {'$arrayElemAt': ["$jobresult.auditts.gran", 0]},          
			  spot_start: {$min: "$jobresult.auditts.date"},
			  spot_finish: {$max: "$jobresult.auditts.date"},
			  granularity: {$arrayElemAt: ["$jobresult.auditts.gran", 0]},
			  jobstartat: 1,
			  jobstarted: 1,
			  jobfinished: 1,
			  jobduration: { $divide: [{"$subtract": ["$jobfinished", "$jobstarted"]}, 1000]},
			  jobwaiting: { $divide: [{"$subtract": ["$jobstarted", "$jobstartat"]}, 1000]},
			  jobtotal: { $divide: [{"$subtract": ["$jobfinished", "$jobstartat" ]}, 1000]}
			}
		},

		// Stage 3
		{
			$unwind: {
			    path : "$spotfile_cost_and_values",
			    preserveNullAndEmptyArrays : true // optional
			}
		},

		// Stage 4
		{
			$match: {
			     "spotfile_values.metric" : {$ne: "cost"},
			}
		},

		// Stage 5
		{
			$unwind: {
			    path : "$spotfile_values",
			    preserveNullAndEmptyArrays : true // optional
			}
		},

		// Stage 6
		{
			$project: {
			         jobid: 1,
			  clientid: 1, 
			  brandid: 1, 
			  jobworktickets: 1,
			  jobtype: 1,
			  job_result: 1,  
			  cluster_info: 1,
			  jobpriority:  1,
			  spotfile_values: "$spotfile_cost_and_values.values",
			  spot_start: 1,
			  spot_finish: 1,
			  totalrowcount: 1,
			  modelrowcount: 1,
			  total_cost: 1,
			  granularity: 1,          
			  spot_start: 1,
			  spot_finish: 1,
			  granularity: 1,
			  jobstartat: 1,
			  jobstarted: 1,
			  jobfinished: 1,
			  jobduration: 1,
			  jobwaiting: 1,
			  jobtotal: 1
			}
		},

		// Stage 7
		{
			$unwind: {
			    path : "$spotfile_values",
			    preserveNullAndEmptyArrays : true // optional
			}
		},

		// Stage 8
		{
			$group: {
			      _id: {
			            clientid: "$clientid", 
			            brandid: "$brandid", 
			            workticketid: "$jobworktickets", 
			         	cluster_info: "$cluster_info",
						jobpriority:  "$jobpriority",
			            spot_start: "$spot_start", 
			            spot_finish: "$spot_finish", 
			            totalrowcount: "$totalrowcount",
			  			modelrowcount: "$modelrowcount",
			  			total_cost: "$total_cost",
			            granularity: "$granularity", 
			            spot_duration: { $add: [{$divide: [{"$subtract": ["$spot_finish", "$spot_start"]}, 1000]}, 86400]},
			            jobtype: "$jobtype",
			            jobstartat: "$jobstartat",
			            jobstarted: "$jobstarted",
			            jobfinished: "$jobfinished",
			            jobduration: "$jobduration",
			            jobwaiting: "$jobwaiting",
			            jobtotal: "$jobtotal", 
			              },
			      spot_total: {$sum: "$spotfile_values"},
			     
			}
		},

		// Stage 9
		{
			$project: {
			      _id:false,
			    clientid: "$_id.clientid",
			    brandid: "$_id.brandid",
			    workticketid: "$_id.workticketid",
			    cluster_info: "$_id.cluster_info",
				jobpriority:  "$_id.jobpriority", 
			    jobtype: "$_id.jobtype",
			    jobstartat: "$_id.jobstartat",
			      jobstarted: "$_id.jobstarted",
			      jobfinished: "$_id.jobfinished",
			      jobduration: "$_id.jobduration",
			      jobwaiting: "$_id.jobwaiting",
			     jobtotal: "$_id.jobtotal", 
			    spot_start: "$_id.spot_start",
			    spot_finish: "$_id.spot_finish",
			    spot_duration: "$_id.spot_duration",
			    granularity: "$_id.granularity",
			    totalrowcount: "$_id.totalrowcount",
			  	modelrowcount: "$_id.modelrowcount",
			  	total_cost: "$_id.total_cost",
			    spot_total: 1,
			
			
			}
		},

		// Stage 10
		{
			$sort: {
			    jobstarted: 1
			}
		},

		// Stage 11
		{
			$unwind: {
			    path : "$workticketid",
			    preserveNullAndEmptyArrays : false // optional
			}
		},

		// Stage 12
		{
			$group: {
			_id: {clientid: "$clientid", 
			      brandid: "$brandid", 
			      workticketid: "$workticketid", 
			      },
			    cluster_info: {"$max": "$cluster_info"},
				jobpriority:  {"$max": "$jobpriority"},   
			    jobcount: {"$sum": 1},
			    workticket_scheduled: {"$min": "$jobstartat"},
			    workticket_started: {"$min": "$jobstarted"},
			    workticket_finished: {"$max": "$jobfinished"},
			    duration: {"$sum": "$jobduration"},
			    waiting: {"$sum": "$jobwaiting"},
			    total: {"$sum": "$jobtotal"},
			    job_times: {"$push": "$jobduration"},
			    jobs: {"$push":  "$jobtype"},
			    spot_start: {"$min": "$spot_start" },
			    spot_finish: {"$max": "$spot_finish"},
			    spot_duration: {"$max": "$spot_duration"},
			    spot_total: {"$max": "$spot_total" },
			    granularity: {"$addToSet": "$granularity"},
			    totalrowcount:{"$max": "$totalrowcount"},
			  	modelrowcount:{"$max": "$modelrowcount"},
			  	total_cost: {"$max": "$total_cost"},
			}
		},

		// Stage 13
		{
			$sort: {
			    granularity: 1,
			}
		},

		// Stage 14
		{
			$project: {
			    _id: false,
			    workticketid: "$_id.workticketid",
			    clientid: "$_id.clientid",
			    brandid: "$_id.brandid",
			    workticket_scheduled: "$workticket_scheduled",
			    workticket_started: "$workticket_started", 
			    workticket_finished: "$workticket_finished",
			    workticket_duration: { $divide: [{"$subtract": ["$workticket_finished", "$workticket_scheduled"]}, 1000]},
			    jobcount: "$jobcount",
			    jobs: 1,
			    total_time_under_calculation: "$duration",
			    total_time_waiting: "$waiting",
			    total_time_in_queue: "$total",   
			    first_job: {$arrayElemAt: ["$jobs", 0]},
			    last_job: {$arrayElemAt: ["$jobs", -1]},
			    last_job_time: {$arrayElemAt: ["$job_times", -1]},
			    second_last_job: {$arrayElemAt: ["$jobs", -2]},
			    spot_start: "$spot_start",
			    spot_finish: "$spot_finish",
			    spot_granularity_0: {$arrayElemAt: ["$granularity", 0]},
			    spot_granularity_1: {$arrayElemAt: ["$granularity", 1]},
			    spot_duration: "$spot_duration",
			    spot_total: "$spot_total",
			    totalrowcount: "$totalrowcount",
			  	modelrowcount: "$modelrowcount",
			  	total_cost: "$total_cost",
			  	cluster_info: 1,
				jobpriority: 1,
			}
		},

		// Stage 15
		{
			$match: {
			   jobs: "LOAD.SPOTS",
			   jobcount: {$gte: 6},
			   $or: [ 
			           {"last_job": { $regex: "INF.SENDEMAIL"}}, 
			           {"last_job": { $regex: "NOTIFY."}}, 
			            {"last_job": { $regex: "PUBLISH."}},
			           {$and: [ {"last_job": "INF.SCRUBDB"},{"second_last_job": {regex: "NOTIFY."}}]},
			           {$and: [ {"last_job": "INF.SCRUBDB"},{"second_last_job": "INF.SENDEMAIL"}]},
			           {$and: [ {"last_job": "INF.SCRUBDB"},{"second_last_job": {regex: "PUBLISH."}}]},
			           ],
			}
		},

		// Stage 16
		{
			$project: {
			    _id: false,
			    workticketid: 1,
			    clientid: 1,
			    brandid: 1,
			    workticket_scheduled: 1,
			    workticket_started: 1, 
			    workticket_finished: 1,
			    workticket_duration: 1,
			    jobcount: 1,
			    jobs: 1,
			    last_job: 1,
			    last_job_time: 1,
			    total_time_under_calculation: 1,
			    total_time_waiting: 1,
			    total_time_in_queue: 1,   
			    spot_start: 1,
			    spot_finish: 1,
			    spot_duration: 1,
			    spot_granularity_1: 1,
			    spot_total: 1,
			    totalrowcount: 1,
			  	modelrowcount: 1,
			  	total_cost: 1,
			  	cluster_info: 1,
				jobpriority: 1,
			}
		},
	],

	// Options
	{
		cursor: {
			batchSize: 50
		},

		allowDiskUse: true
	}

	// Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

);
