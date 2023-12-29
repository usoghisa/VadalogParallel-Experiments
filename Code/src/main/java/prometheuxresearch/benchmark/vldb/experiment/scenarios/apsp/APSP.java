package prometheuxresearch.benchmark.vldb.experiment.scenarios.apsp;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import prometheuxresearch.benchmark.vldb.experiment.scenarios.common.EncoderGenerator;
import prometheuxresearch.benchmark.vldb.experiment.scenarios.common.KeyGroupMapFunction;
import prometheuxresearch.benchmark.vldb.experiment.scenarios.common.SparkSessionManager;
import scala.reflect.ClassTag;

/**
 * 
 * Copyright (C) Prometheux Limited. All rights reserved.
 * 
 * @author Prometheux Limited
 */

public class APSP {
	private String inputFilecontrol;

	public APSP(String inputFilecontrol) {
		this.inputFilecontrol = inputFilecontrol;
	}

	public String evaluate() {

		SparkSessionManager.getInstance().createNewSparkSession();
		SparkSession spark = SparkSessionManager.getInstance().getSparkSession();

		Dataset<Row> edge = spark.read().format("csv").option("inferSchema", "true").load(inputFilecontrol);

		Iterator<Row> it = edge.toLocalIterator();
		Object2ObjectOpenHashMap<Integer, ObjectList<Row>> map = new Object2ObjectOpenHashMap<>();
		while (it.hasNext()) {
			Row row = it.next();
			if (!map.containsKey(row.getInt(0))) {
				map.put(row.getInt(0), new ObjectArrayList<Row>());
			}
			map.get(row.getInt(0)).add(row);
		}

		ClassTag<Object2ObjectOpenHashMap<Integer, ObjectList<Row>>> tag = scala.reflect.ClassTag$.MODULE$
				.apply(map.getClass());
		Broadcast<Object2ObjectOpenHashMap<Integer, ObjectList<Row>>> bVar = spark.sparkContext().broadcast(map, tag);

		edge = edge.alias("own").withColumnRenamed(edge.columns()[0], "edge_0")
				.withColumnRenamed(edge.columns()[1], "edge_1").withColumnRenamed(edge.columns()[2], "edge_2");

		Dataset<Row> APSP = this.withFlatMapGroup(edge, bVar, spark);

		String count = String.valueOf(APSP.count());
		bVar.destroy();
		SparkSessionManager.getInstance().stopSparkSession();
		SparkSessionManager.getInstance().closeSparkSession();
		return count;
	}

	private Dataset<Row> withFlatMapGroup(Dataset<Row> edge,
			Broadcast<Object2ObjectOpenHashMap<Integer, ObjectList<Row>>> bVar, SparkSession spark) {
		List<Integer> keyPositions = Arrays.asList(0);
		Encoder<Row> keyEncoder = EncoderGenerator.createEncoderFromEncoder(edge.encoder(), keyPositions);
		KeyGroupMapFunction keys = new KeyGroupMapFunction(Arrays.asList(0));
		APSPFlatMapGroup APSPFlatMapGroup = new APSPFlatMapGroup(bVar);
		Dataset<Row> apsp = edge.groupByKey(keys, keyEncoder).flatMapGroups(APSPFlatMapGroup, edge.encoder());
		return apsp;
	}

}

