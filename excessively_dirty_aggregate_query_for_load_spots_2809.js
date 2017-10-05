db.Loadspots_practice.aggregate(

	// Pipeline
	[
		// Stage 1
		{
			$match: {
			    jobtype: "LOAD.SPOTS"
			}
		},

		// Stage 2
		{
			$project: {
			  job_id: "$_id",	
			  clientid: 1, 
			  brandid: 1, 
			  jobworktickets: 1,
			  jobtype: 1,
			  jobresult: "$jobresult.auditts",
			}
		},

		// Stage 3
		{
			$sort: {
			  "_id.jobresult.date": -1
			}
		},

		// Stage 4
		{
			$project: {
			  job_id: 1,	
			  clientid: 1, 
			  brandid: 1, 
			  jobworktickets: 1,
			  jobtype: 1,
			  jobresult: 1, 
			  spot_start: {$min: "$jobresult.date"},
			  spot_finish: {$max: "$jobresult.date"},
			}
		},

		// Stage 5
		{
			$project: {	
			  	job_id: 1,
			  	clientid: 1, 
			  	brandid: 1,
			  	jobworktickets: 1, 
			  	jobtype: 1,
			  	jobresult: 1, 
			  	spot_start: 1,
			 	spot_finish: 1,
				jobresult: "$jobresult.values"
			}
		},

		// Stage 6
		{
			$unwind: {
				path: "$jobworktickets"
			}
		},

		// Stage 7
		{
			$project: {
			  job_id: 1,
			  clientid: 1, 
			  brandid: 1, 
			  jobworktickets: 1,
			  jobtype: 1,
			  jobresult: 1, 
			  spot_start: 1,
			  spot_finish: 1,
			}
		},

		// Stage 8
		{
			$unwind: {
			    path : "$jobresult",
			}
		},

		// Stage 9
		{
			$unwind: {
			    path : "$jobresult"
			}
		},

		// Stage 10
		{
			$group: {
				_id: {job_id: "$job_id", client: "$clientid", brandid: "$brandid", workticket: "$jobworktickets", spot_start: "$spot_start", spot_finish: "$spot_finish"},
				jobresult_total: {$sum: "$jobresult"}
			}
		},

		// Stage 11
		{
			$project: {
			  	job: "$_id.job_id",
			    client: "$_id.client",
			    brand: "$_id.brandid",
			    workticket: "$_id.workticket",
			    earliest_spot_file: "$_id.spot_start",
			    latest_spot_file: "$_id.spot_finish",
			    total_spots: "$jobresult_total"
			}
		},

	]

	// Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

);
