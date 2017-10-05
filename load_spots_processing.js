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
			$unwind: {
			    path : "$jobresults.auditts",
			    preserveNullAndEmptyArrays : false // optional
			}
		},

		// Stage 3
		{
			$project: {
			    // specifications
			}
		},

	]

	// Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

);
