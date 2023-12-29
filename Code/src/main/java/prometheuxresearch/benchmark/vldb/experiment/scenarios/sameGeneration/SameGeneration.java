package prometheuxresearch.benchmark.vldb.experiment.scenarios.sameGeneration;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import scala.reflect.ClassTag;
import prometheuxresearch.benchmark.vldb.experiment.scenarios.common.EncoderGenerator;
import prometheuxresearch.benchmark.vldb.experiment.scenarios.common.KeyGroupMapFunction;
import prometheuxresearch.benchmark.vldb.experiment.scenarios.common.SparkSessionManager;

/**
 * 
 * Copyright (C) Prometheux Limited. All rights reserved.
 * 
 * @author Prometheux Limited
 */

public class SameGeneration {
	private String inputFilePath;

	public SameGeneration(String inputFilePath) {
		this.inputFilePath = inputFilePath;
	}

	public String evaluate() {

		SparkSessionManager.getInstance().createNewSparkSession();
		SparkSession spark = SparkSessionManager.getInstance().getSparkSession();

		Dataset<Row> arc = spark.read().format("csv").option("inferSchema", "true").load(inputFilePath);

		Iterator<Row> it = arc.toLocalIterator();
		Object2ObjectOpenHashMap<Integer, ObjectOpenHashSet<Row>> map = new Object2ObjectOpenHashMap<>();
		while (it.hasNext()) {
			Row row = it.next();
			if (!map.containsKey(row.getInt(0))) {
				map.put(row.getInt(0), new ObjectOpenHashSet<Row>());
			}
			map.get(row.getInt(0)).add(row);
		}

		ClassTag<Object2ObjectOpenHashMap<Integer, ObjectOpenHashSet<Row>>> tag = scala.reflect.ClassTag$.MODULE$
				.apply(map.getClass());
		Broadcast<Object2ObjectOpenHashMap<Integer, ObjectOpenHashSet<Row>>> bVar = spark.sparkContext().broadcast(map,
				tag);

		Dataset<Row> sg = this.withFlatMapGroup(arc, bVar, spark);
		sg = sg.distinct();

		String count = String.valueOf(sg.count());
		bVar.destroy();
		SparkSessionManager.getInstance().stopSparkSession();
		SparkSessionManager.getInstance().closeSparkSession();
		return count;
	}

	private Dataset<Row> withFlatMapGroup(Dataset<Row> arc,
			Broadcast<Object2ObjectOpenHashMap<Integer, ObjectOpenHashSet<Row>>> bVar, SparkSession spark) {
		KeyGroupMapFunction keys = new KeyGroupMapFunction(Arrays.asList(1));
		List<Integer> keyPositions = Arrays.asList(1);
		Encoder<Row> keyEncoder = EncoderGenerator.createEncoderFromEncoder(arc.encoder(), keyPositions);
		SameGenerationFlatMapGroup tc = new SameGenerationFlatMapGroup(bVar);
		Dataset<Row> sg = arc.groupByKey(keys, keyEncoder).flatMapGroups(tc, arc.encoder());
		return sg;
	}

}
