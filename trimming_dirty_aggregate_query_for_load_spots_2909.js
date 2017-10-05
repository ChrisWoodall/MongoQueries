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
			$project: {
			  job_id: 1,	
			  clientid: 1, 
			  brandid: 1, 
			  jobworktickets: 1,
			  jobtype: 1,
			  jobresult_values: "$jobresult.values", 
			  spot_start: {$min: "$jobresult.date"},
			  spot_finish: {$max: "$jobresult.date"},
			  granularity: {$arrayElemAt: ["$jobresult.gran", 0]}
			}
		},

		// Stage 4
		{
			$unwind: {
				//path: "$jobworktickets",
				path : "$jobresult_values",	
			}
		},

		// Stage 5
		{
			$unwind: {
			  path : "$jobresult_values"
			}
		},

		// Stage 6
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

		// Stage 7
		{
			$match: {
			
			}
		},

	]

	// Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

);
