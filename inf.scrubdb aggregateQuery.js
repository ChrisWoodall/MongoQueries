db.jobqueue.aggregate(

	// Pipeline
	[
		// Stage 1
		{
			$match: {
				 jobstartat: {"$lt": ISODate('2017-09-20'), "$gt": ISODate('2017-09-09')},
			     jobtype: "INF.SCRUBDB"
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
			  jobstartat: 1,
			  jobstarted: 1,
			  jobfinished: 1,
			  jobduration: { $divide: [{"$subtract": ["$jobfinished", "$jobstarted"]}, 1000]},
			  jobwaiting: { $divide: [{"$subtract": ["$jobstarted", "$jobstartat"]}, 1000]},
			  jobtotal: { $divide: [{"$subtract": ["$jobfinished", "$jobstartat" ]}, 1000]}
			}
		},

	]

	// Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

);
