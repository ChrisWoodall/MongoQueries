db.jobqueue.aggregate(

	// Pipeline
	[
		// Stage 1
		{
			$match: {
			    jobstartat: {"$lt": ISODate('2017-09-12'), "$gt": ISODate('2017-09-09')},
			    //jobtype:/EXPORT/
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
			  spotfile_values: "$jobresult.auditts.values", 
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
			    path : "$spotfile_values",
			    
			    preserveNullAndEmptyArrays : true // optional
			}
		},

		// Stage 4
		{
			$unwind: {
			    path : "$spotfile_values",
			    preserveNullAndEmptyArrays : true // optional
			}
		},

		// Stage 5
		{
			$group: {
				  _id: {
				    		clientid: "$clientid", 
				        brandid: "$brandid", 
				        workticketid: "$jobworktickets", 
				        spot_start: "$spot_start", 
				        spot_finish: "$spot_finish", 
				        granularity: "$granularity", 
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

		// Stage 6
		{
			$project: {
			  	_id:false,
			    clientid: "$_id.clientid",
			    brandid: "$_id.brandid",
			    workticketid: "$_id.workticketid", 
			    jobtype: "$_id.jobtype",
			    spot_start: "$_id.spot_start",
			    spot_finish: "$_id.spot_finish",
			    granularity: "$_id.granularity",
			    spot_total: 1,
			    jobstartat: "$_id.jobstartat",
			  	jobstarted: "$_id.jobstarted",
			  	jobfinished: "$_id.jobfinished",
			  	jobduration: "$_id.jobduration",
			  	jobwaiting: "$_id.jobwaiting",
			 	jobtotal: "$_id.jobtotal", 
			}
			
			
		},

		// Stage 7
		{
			$unwind: {
			    path : "$workticketid",
			    preserveNullAndEmptyArrays : false // optional
			}
		},

		// Stage 8
		{
			$group: {
			_id: {clientid: "$clientid", brandid: "$brandid", workticketid: "$workticketid", spot_start: "$spot_start", spot_finish: "$spot_finish", spot_granularity: "$granularity", spot_total: "$spot_total"},
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

		// Stage 9
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
			    s: "$jobcount",
				jobs: 1,
			    total_time_under_calculation: "$duration",
			    total_time_waiting: "$waiting",
			    total_time_in_queue: "$total",   
			    first_job: {$arrayElemAt: ["$jobs", 0]},
			    last_job: {$arrayElemAt: ["$jobs", -1]},
			    spot_start: "$_id.spot_start",
			    spot_finish: "$_id.spot_finish",
			    spot_granularity: "$_id.spot_granularity",
			    spot_result_values: "$_id.spot_total"
			}
		},

		// Stage 10
		{
			$match: {
			   jobs: "MODEL.SPOT",
			   //total_number_of_jobs: {$gte: 6},
			   //$or: [ {"last_job": { $regex: "INF."}}, {"last_job": { $regex: "NOTIFY."}}, {"last_job": { $regex: "PUBLISH."}}]
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

		// Stage 12
		{
			$match: {
			
			}
		},

		// Stage 13
		{
			$match: {
			
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
