db.oneWeek.aggregate(

	// Pipeline
	[
		// Stage 1
		{
			$match: {
			jobstartat: {"$lt": ISODate('2017-09-11')},
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
			    jobwaiting: { $divide: [{"$subtract": ["$jobstarted", "$jobstartat"]}, 1000]}
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
			    count: "$jobcount",
			    scheduled: "$jobstartat",
			    started: "$jobstarted", 
			    finished: "$jobfinished",
			    duration: 1,
			    waiting: 1,
			    jobs: 1
			}
		},

		// Stage 6
		{
			$match: {
			   jobs: {"$elemMatch": {$eq: "LOAD.SPOTS"}}
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
