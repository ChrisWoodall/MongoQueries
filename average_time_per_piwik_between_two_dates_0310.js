db.jobqueue.aggregate(

	// Pipeline
	[
		// Stage 1
		{
			$match: {
			    jobstartat: { "$lt": ISODate('2017-09-21'), "$gt": ISODate("2017-09-20")},
			    jobstatus: "DONE",
			    jobtype: "LOAD.SESSIONSPIWIK"
			}
			
		},

		// Stage 2
		{
			$project: {
			      jobfinished: 1,
			      jobstartat: 1,
			      jobstarted: 1,
			        total_job_time_seconds: { $divide: [{"$subtract": ["$jobfinished", "$jobstarted"]}, 1000]},
			     _id: 1,
			    	clientid: 1,
			      brandid: 1,
			      jobworktickets: 1,
			      jobtype: 1
			}
		},

		// Stage 3
		{
			$group: {
			    _id: {client: "$clientid", brandid: "$brandid",},
			    piwiks: {$push: "$_id"},
			    average_time: { $avg: "$total_job_time_seconds"},
			    total_time: {$sum: "$total_job_time_seconds"},
			    standard_deviation_of_time: {$stdDevPop: "$total_job_time_seconds"}
			}
		},

		// Stage 4
		{
			$project: {
			    _id: false,
			    client: "$_id.client",
			    brand: "$_id.brandid",
			    piwiks: 1,
			    average_time_per_piwik: "$average_time",
			    total_time: "$total_time",
			    standard_deviation_of_time: "$standard_deviation_of_time"
			}
		},

	]

	// Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

);
