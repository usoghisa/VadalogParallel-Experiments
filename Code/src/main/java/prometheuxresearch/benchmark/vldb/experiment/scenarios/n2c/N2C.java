package prometheuxresearch.benchmark.vldb.experiment.scenarios.n2c;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

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

public class N2C {
	private String inputFilePath;

	public N2C(String inputFilePath) {
		this.inputFilePath = inputFilePath;
	}

	public String evaluate() {

		SparkSessionManager.getInstance().createNewSparkSession();
		SparkSession spark = SparkSessionManager.getInstance().getSparkSession();

		Dataset<Row> edge = spark.read().format("csv").option("inferSchema", "true").load(inputFilePath);

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

		Dataset<Row> N2C = edge.alias("n2c").withColumnRenamed(edge.columns()[0], "n2c_0")
				.withColumnRenamed(edge.columns()[1], "n2c_1");

		N2C = this.withFlatMapGroup(N2C, bVar, spark);
		// outside the recursion, we query the even paths with same source and target
		// (we do not have bipartition)
//		N2C = N2C.where(new Column("n2c_2").equalTo("EVEN").and(new Column("n2c_0").equalTo(new Column("n2c_1"))));

		String count = String.valueOf(N2C.count());
		bVar.destroy();
		SparkSessionManager.getInstance().stopSparkSession();
		SparkSessionManager.getInstance().closeSparkSession();
		return count;
	}

	private Dataset<Row> withFlatMapGroup(Dataset<Row> path,
			Broadcast<Object2ObjectOpenHashMap<Integer, ObjectList<Row>>> bVar, SparkSession spark) {
		List<Integer> keyPositions = Arrays.asList(0);
		Encoder<Row> keyEncoder = EncoderGenerator.createEncoderFromEncoder(path.encoder(), keyPositions);

		StructType N2CStructType = new StructType();
		N2CStructType = N2CStructType.add("n2c_0", DataTypes.IntegerType, false);
		N2CStructType = N2CStructType.add("n2c_1", DataTypes.IntegerType, false);
		N2CStructType = N2CStructType.add("n2c_2", DataTypes.StringType, false);
		ExpressionEncoder<Row> N2CEncoder = RowEncoder.apply(N2CStructType);

		KeyGroupMapFunction keys = new KeyGroupMapFunction(Arrays.asList(0));
		N2CFlatMapGroup N2CFlatMapGroup = new N2CFlatMapGroup(bVar);
		path = path.groupByKey(keys, keyEncoder).flatMapGroups(N2CFlatMapGroup, N2CEncoder);
		return path;
	}
}
