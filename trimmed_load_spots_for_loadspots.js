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
			  //jobresult: "$jobresult.auditts",
			  jobresult_values: "$jobresult.auditts.values", 
			  spot_start: {$min: "$jobresult.auditts.date"},
			  spot_finish: {$max: "$jobresult.auditts.date"},
			  granularity: {$arrayElemAt: ["$jobresult.auditts.gran", 0]}
			}
		},

		// Stage 3
		{
			$unwind: {
			    //path: "$jobworktickets",
			    path : "$jobresult_values",    
			}
		},

		// Stage 4
		{
			$unwind: {
			  path : "$jobresult_values"
			}
		},

		// Stage 5
		{
			$group: {
			    _id: {job_id: "$job_id", client: "$clientid", brandid: "$brandid", workticket: "$jobworktickets", spot_start: "$spot_start", spot_finish: "$spot_finish"},
			    jobresult_total: {$sum: "$jobresult_values"}
			}
		},

		// Stage 6
		{
			$project: 
			{
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
