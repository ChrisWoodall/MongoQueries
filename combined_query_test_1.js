db.Loadspots_practice.aggregate(

	// Pipeline
	[
		// Stage 1
		{
			$match: {
			    jobstartat: {"$lt": ISODate('2017-09-20'), "$gt": ISODate('2017-09-09')},
			    //jobtype:/EXPORT/
			}
		},

		// Stage 2
		{
			$project: {
			  clientid: 1, 
			  brandid: 1, 
			  jobworktickets: 1,
			  jobtype: 1,
			  jobresult_values: "$jobresult.auditts.values", 
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
			    path : "$jobworktickets",
			    
			    preserveNullAndEmptyArrays : false // optional
			}
		},

		// Stage 4
		{
			$group: {
			_id: {client: "$clientid", brandid: "$brandid", workticket: "$jobworktickets", spot_start: "$spot_start", spot_finish: "$spot_finish", spot_granularity: "$granularity", spot_result_values: "$jobresult_values"},
			jobcount: {"$sum": 1},
			workticket_scheduled: {"$min": "$jobstartat"},
			workticket_started: {"$min": "$jobstarted"},
			workticket_finished: {"$max": "$jobfinished"},
			duration: {"$sum": "$jobduration"},
			waiting: {"$sum": "$jobwaiting"},
			total: {"$sum": "$jobtotal"},
			jobs: {"$push":"$jobtype"}
			}
		},

		// Stage 5
		{
			$project: {
			    _id: false,
			    client: "$_id.client",
			    brand: "$_id.brandid",
			    workticket: "$_id.workticket",
			    total_number_of_jobs: "$jobcount",
			    workticket_scheduled: "$workticket_scheduled",
			    workticket_started: "$workticket_started", 
			    workticket_finished: "$workticket_finished",
			    workticket_duration: { $divide: [{"$subtract": ["$workticket_finished", "$workticket_scheduled"]}, 1000]},
			    total_time_under_calculation: "$duration",
			    total_time_waiting: "$waiting",
			    total_time_in_queue: "$total", 
			    jobs: 1,
			    first_job: {$arrayElemAt: ["$jobs", 0]},
			    last_job: {$arrayElemAt: ["$jobs", -1]},
			    spot_start: "$_id.spot_start",
			    spot_finish: "$_id.spot_finish",
			    spot_granularity: "$_id.spot_granularity",
			    spot_result_values: "$_id.spot_result_values"
			}
		},

		// Stage 6
		{
			$unwind: {
			    path : "$spot_result_values"
			}
		},

		// Stage 7
		{
			$unwind: {
			    path : "$spot_result_values",
			}
		},

		// Stage 8
		{
			$group: {
				 _id: {
				    client: "$client",
			    		brand: "$brand",
			    		workticket: "$workticket",
			    		total_number_of_jobs: "$total_number_of_jobs",
			    		workticket_scheduled: "$workticket_scheduled",
			    		workticket_started: "$workticket_started", 
			    		workticket_finished: "$workticket_finished",
			    		workticket_duration: "$workticket_duration",
			    		total_time_under_calculation: "$total_time_under_calculation",
			    		total_time_waiting: "$total_time_waiting",
			    		total_time_in_queue: "$total_time_in_queue", 
			    		jobs: "$jobs",
			    		first_job: "$first_job",
			    		last_job: "$last_job",
			    		spot_start: "$spot_start",
			    		spot_finish: "$spot_finish",
			    		spot_granularity: "$spot_granularity",	   
				   },
					spot_result_total: {$sum: "$spot_result_values"}
			}
		},

		// Stage 9
		{
			$project: {
			   		_id: false,
			   		client: "$_id.client",
			    		brand: "$_id.brand",
			    		workticket: "$_id.workticket",
			    		total_number_of_jobs: "$_id.total_number_of_jobs",
			    		workticket_scheduled: "$_id.workticket_scheduled",
			    		workticket_started: "$_id.workticket_started", 
			    		workticket_finished: "$_id.workticket_finished",
			    		workticket_duration: "$_id.workticket_duration",
			    		total_time_under_calculation: "$_id.total_time_under_calculation",
			    		total_time_waiting: "$_id.total_time_waiting",
			    		total_time_in_queue: "$_id.total_time_in_queue", 
			    		jobs: "$_id.jobs",
			    		first_job: "$_id.first_job",
			    		last_job: "$_id.last_job",
			    		spot_start: "$_id.spot_start",
			    		spot_finish: "$_id.spot_finish",
			    		spot_granularity: "$_id.spot_granularity",	   
					spot_result_total: "$spot_result_total"
			}
		},

		// Stage 10
		{
			$match: {
			   jobs: "LOAD.SPOTS",
			   total_number_of_jobs: {$gte: 6},
			   $or: [ {"last_job": { $regex: "INF."}}, {"last_job": { $regex: "NOTIFY."}}, {"last_job": { $regex: "PUBLISH."}}]
			}
		},

		// Stage 11
		{
			$project: {
			    _id: false,
			    client: 1,
			    brand: 1,
			    workticket: 1,
			    total_number_of_jobs: 1,
			    workticket_scheduled: 1,
			    workticket_started: 1, 
			    workticket_finished: 1,
			    workticket_duration: 1,
			    total_time_under_calculation: 1,
			    total_time_waiting: 1,
			    total_time_in_queue: 1, 
			    jobs: 1,
			    first_job: 1,
			    last_job: 1
			}
		},
	],

	// Options
	{
		cursor: {
			batchSize: 50
		}
	}

	// Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

);
