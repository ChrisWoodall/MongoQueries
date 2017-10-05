db['oneWeek jobtickets'].aggregate(

	// Pipeline
	[
		// Stage 1
		{
			$match: {
			jobstartat: {"$lt": ISODate('2017-09-18')},
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
			jobstartat: {"$min": "$jobstartat"},
			jobstarted: {"$min": "$jobstarted"},
			jobfinished: {"$max": "$jobfinished"},
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
			    job_scheduled: "$jobstartat",
			    job_started: "$jobstarted", 
			    job_finished: "$jobfinished",
			    duration: 1,
			    waiting: 1,
			    total: 1,
			    jobs: 1
			}
		},

		// Stage 6
		{
			$match: {
			   jobs: {"$elemMatch": {$eq: "LOAD.SPOTS"}}
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
			   job_scheduled: 1,
			   job_started: 1, 
			   job_finished: 1,
			   jobs: 1,
			   total_time_under_calculation: "$duration",
			   total_time_waiting: "$waiting",
			   total_time_in_queue: "$total", 
			   last_job: {$arrayElemAt: ["$jobs", -1]}
			}
		},

		// Stage 8
		{
			$match: {
			   last_job: { $in: ["INF.SENDEMAIL", "NOTIFY.DATA"]}
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
