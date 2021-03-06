db.partJQ.aggregate(

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
			_id: {client: "$clientid", brandid: "$brandid", workticket: "$jobworktickets"},
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
			    last_job: {$arrayElemAt: ["$jobs", -1]}
			}
		},

		// Stage 6
		{
			$match: {
			   jobs: "LOAD.SPOTS",
			   total_number_of_jobs: {$gte: 6},
			   last_job: { $in: [/^INF/, /^NOTIFY/, /^PUBLISH/]}
			}
		},

		// Stage 7
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
