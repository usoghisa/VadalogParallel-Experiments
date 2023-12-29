package prometheuxresearch.benchmark.vldb.experiment.scenarios.companyControl;

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

public class CompanyControl {
	private String inputFilecontrol;

	public CompanyControl(String inputFilecontrol) {
		this.inputFilecontrol = inputFilecontrol;
	}

	public String evaluate() {

		SparkSessionManager.getInstance().createNewSparkSession();
		SparkSession spark = SparkSessionManager.getInstance().getSparkSession();

		Dataset<Row> own = spark.read().format("csv").option("inferSchema", "true").load(inputFilecontrol);

		Iterator<Row> it = own.toLocalIterator();
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

		own = own.alias("own").withColumnRenamed(own.columns()[0], "own_0").withColumnRenamed(own.columns()[1], "own_1")
				.withColumnRenamed(own.columns()[2], "own_2");

		Dataset<Row> control = this.withFlatMapGroup(own, bVar, spark);

		String count = String.valueOf(control.count());
		bVar.destroy();
		SparkSessionManager.getInstance().stopSparkSession();
		SparkSessionManager.getInstance().closeSparkSession();
		return count;
	}

	private Dataset<Row> withFlatMapGroup(Dataset<Row> own,
			Broadcast<Object2ObjectOpenHashMap<Integer, ObjectList<Row>>> bVar, SparkSession spark) {
		List<Integer> keyPositions = Arrays.asList(0);
		Encoder<Row> keyEncoder = EncoderGenerator.createEncoderFromEncoder(own.encoder(), keyPositions);
		KeyGroupMapFunction keys = new KeyGroupMapFunction(Arrays.asList(0));
		CompanyControlFlatMapGroup cc = new CompanyControlFlatMapGroup(bVar);
		Dataset<Row> control = own.groupByKey(keys, keyEncoder).flatMapGroups(cc, own.encoder());
		return control;
	}

}
