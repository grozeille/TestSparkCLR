using System;
using System.Collections.Generic;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Services;
using Microsoft.Spark.CSharp.Sql;
using Newtonsoft.Json.Linq;

namespace TestSparkCLR
{
	[Serializable]
	public class SparkJob
	{
		[NonSerialized]
		private static ILoggerService Logger;

		public static void Main(string[] args)
		{
			LoggerServiceFactory.SetLoggerService(Log4NetLoggerService.Instance); //this is optional - DefaultLoggerService will be used if not set
			Logger = LoggerServiceFactory.GetLogger(typeof(SparkJob));

			var sparkContext = new SparkContext(new SparkConf());

			var path = "/user/mathias/beermap.json";

			var tests = new List<Action<SparkContext, string>>();
			tests.Add(SparkJob.RunWithRdd);
			tests.Add(SparkJob.RunWithDataframeAndRdd);
			tests.Add(SparkJob.RunWithDataframe);
			tests.Add(SparkJob.RunWithDataframeSQL);

			foreach (var test in tests)
			{
				try
				{
					test.Invoke(sparkContext, path);

				}
				catch (Exception ex)
				{
					Logger.LogError("Error during Spark job");
					Logger.LogException(ex);
				}
			}

			sparkContext.Stop();

		}

		public static void RunWithDataframeAndRdd(SparkContext sparkContext, string path)
		{
			Logger.LogInfo("RunWithDataframeAndRdd");
			var sqlContext = new SqlContext(sparkContext);

			var beerMap = sqlContext.Read().Json(path);
			//beerMap.ShowSchema();

			// read DataFrame Rows to extract the BEERS
			var allBeer = beerMap.Rdd.FlatMap(row =>
			{
				try
				{
					var propertiesField = row.GetAs<Row>("properties");
					var beersField = propertiesField.GetAs<object[]>("BEERS");

					LoggerHelper.GetOrCreateLogger().LogDebug("Reading a row in C# Lambda");

					return Array.ConvertAll<object, string>(beersField, o => (string)o);
				}
				catch (Exception e)
				{
					LoggerHelper.GetOrCreateLogger().LogError("Something went wrong");
					LoggerHelper.GetOrCreateLogger().LogException(e);
				}

				return new String[0];

			});

			RunMapReduce(allBeer);

		}

		public static void RunWithRdd(SparkContext sparkContext, string path)
		{
			Logger.LogInfo("RunWithRdd");
			var beerMapLines = sparkContext.TextFile(path);

			var allBeer = beerMapLines.FlatMap((string arg) =>
			{
				JObject o = JObject.Parse(arg);
				JToken token = o.SelectToken("$.properties.BEERS");
				var beers = token.Values<String>();
				return beers;

			});

			RunMapReduce(allBeer);
		}

		public static void RunMapReduce(RDD<String> allBeer)
		{
			// classical MapReduce
			var allBeerMap = allBeer.Map(b => new KeyValuePair<string, int>(b, 1));

			var allBeerReduce = allBeerMap.ReduceByKey((v1, v2) => v1 + v2);

			Logger.LogInfo("10 MapReduce result:");
			foreach (var kv in allBeerReduce.Take(10))
			{
				Logger.LogInfo(kv.Key + " : " + kv.Value);
			}
		}

		public static void RunWithDataframe(SparkContext sparkContext, string path)
		{
			Logger.LogInfo("RunWithDataframe");
			var sqlContext = new SqlContext(sparkContext);

			var beerMap = sqlContext.Read().Json(path);

			var beers = beerMap.Select(Functions.Explode(Functions.Col("properties.BEERS")).Alias("BEERS"));

			var countBeer = beers.GroupBy("BEERS").Count();

			countBeer.Limit(10).Show();
		}

		public static void RunWithDataframeSQL(SparkContext sparkContext, string path)
		{
			Logger.LogInfo("RunWithDataframe");
			var sqlContext = new SqlContext(sparkContext);

			var beerMap = sqlContext.Read().Json(path);

			beerMap.RegisterTempTable("beerMapTable");

			sqlContext.Sql(@"
				SELECT count(*) as count, beer 
				FROM beerMapTable 
				LATERAL VIEW explode(properties.beers) beersTable AS beer 
				GROUP BY beer
				ORDER BY count(*) DESC
				").Limit(10).Show();
		}

		[Serializable]
		public class LoggerHelper
		{
			[NonSerialized]
			private static ILoggerService LocalLogger;

			public static ILoggerService GetOrCreateLogger()
			{
				if (LocalLogger == null)
				{
					LocalLogger = LoggerServiceFactory.GetLogger(typeof(SparkJob));
				}
				return LocalLogger;
			}
		}
	}
}
