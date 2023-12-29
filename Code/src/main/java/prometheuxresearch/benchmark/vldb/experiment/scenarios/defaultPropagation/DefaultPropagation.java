package prometheuxresearch.benchmark.vldb.experiment.scenarios.defaultPropagation;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
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

public class DefaultPropagation {
	private String inputCreditExposure;
	private String inputLoan;
	private String inputSecurity;

	public DefaultPropagation(String inputCreditExposure, String inputLoan, String inputSecurity) {
		super();
		this.inputCreditExposure = inputCreditExposure;
		this.inputLoan = inputLoan;
		this.inputSecurity = inputSecurity;
	}

	public String evaluate() {

		SparkSessionManager.getInstance().createNewSparkSession();
		SparkSession spark = SparkSessionManager.getInstance().getSparkSession();

		Dataset<Row> creditExposure = spark.read().format("csv").option("inferSchema", "true")
				.load(this.inputCreditExposure);
		Dataset<Row> loan = spark.read().format("csv").option("inferSchema", "true").load(this.inputLoan);
		Dataset<Row> security = spark.read().format("csv").option("inferSchema", "true").load(this.inputSecurity);

		Iterator<Row> it = loan.toLocalIterator();
		Object2ObjectOpenHashMap<Integer, ObjectList<Row>> loanIndex = buildIndex(it);
		it = security.toLocalIterator();
		Object2ObjectOpenHashMap<Integer, ObjectList<Row>> securityIndex = buildIndex(it);

		ClassTag<Object2ObjectOpenHashMap<Integer, ObjectList<Row>>> tag = scala.reflect.ClassTag$.MODULE$
				.apply(loanIndex.getClass());
		Broadcast<Object2ObjectOpenHashMap<Integer, ObjectList<Row>>> bLoan = spark.sparkContext().broadcast(loanIndex,
				tag);

		tag = scala.reflect.ClassTag$.MODULE$.apply(securityIndex.getClass());
		Broadcast<Object2ObjectOpenHashMap<Integer, ObjectList<Row>>> bSecurity = spark.sparkContext()
				.broadcast(securityIndex, tag);

		creditExposure = creditExposure.alias("creditExposure")
				.withColumnRenamed(creditExposure.columns()[0], "creditExposure_0")
				.withColumnRenamed(creditExposure.columns()[1], "creditExposure_1");

		Dataset<String> defaults = this.withFlatMapGroup(creditExposure, bLoan, bSecurity, spark);

		String count = String.valueOf(defaults.count());
		bLoan.destroy();
		bSecurity.destroy();
		SparkSessionManager.getInstance().stopSparkSession();
		SparkSessionManager.getInstance().closeSparkSession();
		return count;
	}

	private Dataset<String> withFlatMapGroup(Dataset<Row> creditExposure,
			Broadcast<Object2ObjectOpenHashMap<Integer, ObjectList<Row>>> bLoan,
			Broadcast<Object2ObjectOpenHashMap<Integer, ObjectList<Row>>> bSecurity, SparkSession spark) {
		List<Integer> keyPositions = Arrays.asList(0);
		Encoder<Row> keyEncoder = EncoderGenerator.createEncoderFromEncoder(creditExposure.encoder(), keyPositions);
		KeyGroupMapFunction keys = new KeyGroupMapFunction(Arrays.asList(0));
		DefaultPropagationFlatMapGroup dp = new DefaultPropagationFlatMapGroup(bLoan, bSecurity);

		Dataset<String> defaults = creditExposure.groupByKey(keys, keyEncoder).flatMapGroups(dp, Encoders.STRING());
		return defaults;
	}

	private Object2ObjectOpenHashMap<Integer, ObjectList<Row>> buildIndex(Iterator<Row> it) {
		Object2ObjectOpenHashMap<Integer, ObjectList<Row>> map = new Object2ObjectOpenHashMap<>();
		while (it.hasNext()) {
			Row row = it.next();
			if (!map.containsKey(row.getInt(0))) {
				map.put(row.getInt(0), new ObjectArrayList<Row>());
			}
			map.get(row.getInt(0)).add(row);
		}
		return map;
	}

}
