package prometheuxresearch.benchmark.vldb.experiment.scenarios.psc;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
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

public class PSC {

	private String inputPerson;
	private String inputKeyPerson;
	private String inputCompany;
	private String inputControl;

	public PSC(String inputPerson, String inputKeyPerson, String inputCompany, String inputControl) {
		super();
		this.inputPerson = inputPerson;
		this.inputKeyPerson = inputKeyPerson;
		this.inputCompany = inputCompany;
		this.inputControl = inputControl;
	}

	public String evaluate() {

		SparkSessionManager.getInstance().createNewSparkSession();
		SparkSession spark = SparkSessionManager.getInstance().getSparkSession();

		Dataset<Row> person = spark.read().format("csv").option("inferSchema", "true").load(this.inputPerson);

		Dataset<Row> keyPerson = spark.read().format("csv").option("inferSchema", "true").load(this.inputKeyPerson);

		Dataset<Row> company = spark.read().format("csv").option("inferSchema", "true").load(this.inputCompany);

		Dataset<Row> control = spark.read().format("csv").option("inferSchema", "true").load(this.inputControl);

		Iterator<Row> it = control.toLocalIterator();
		Object2ObjectOpenHashMap<String, ObjectList<Row>> controlIndex = buildIndex(it);

		ClassTag<Object2ObjectOpenHashMap<String, ObjectList<Row>>> tag = scala.reflect.ClassTag$.MODULE$
				.apply(controlIndex.getClass());
		Broadcast<Object2ObjectOpenHashMap<String, ObjectList<Row>>> bControl = spark.sparkContext()
				.broadcast(controlIndex, tag);

		it = person.toLocalIterator();
		Object2ObjectOpenHashMap<String, ObjectList<Row>> personIndex = buildIndex(it);

		tag = scala.reflect.ClassTag$.MODULE$.apply(personIndex.getClass());
		Broadcast<Object2ObjectOpenHashMap<String, ObjectList<Row>>> bPerson = spark.sparkContext()
				.broadcast(personIndex, tag);

		Dataset<String> psc = this.withFlatMapGroup(keyPerson, bPerson, company, bControl, spark);

		String count = String.valueOf(psc.count());
		bControl.destroy();
		bPerson.destroy();
		SparkSessionManager.getInstance().stopSparkSession();
		SparkSessionManager.getInstance().closeSparkSession();
		return count;

	}

	private Dataset<String> withFlatMapGroup(Dataset<Row> keyPerson,
			Broadcast<Object2ObjectOpenHashMap<String, ObjectList<Row>>> bPerson, Dataset<Row> company,
			Broadcast<Object2ObjectOpenHashMap<String, ObjectList<Row>>> bControl, SparkSession spark) {
		KeyGroupMapFunction keysKeyPerson = new KeyGroupMapFunction(Arrays.asList(0));
		KeyGroupMapFunction keysCompany = new KeyGroupMapFunction(Arrays.asList(0));
		PSCCoGroup pscCoGroup = new PSCCoGroup(bControl, bPerson);

		List<Integer> keyPositions = Arrays.asList(0);
		Encoder<Row> keyPersonsEncoder = EncoderGenerator.createEncoderFromEncoder(keyPerson.encoder(), keyPositions);
		Encoder<Row> keyCompanyEncoder = EncoderGenerator.createEncoderFromEncoder(company.encoder(), keyPositions);

		KeyValueGroupedDataset<Row, Row> groupedKP = keyPerson.groupByKey(keysKeyPerson, keyPersonsEncoder);
		KeyValueGroupedDataset<Row, Row> groupedCompany = company.groupByKey(keysCompany, keyCompanyEncoder);

		Dataset<String> psc = groupedKP.cogroup(groupedCompany, pscCoGroup, Encoders.STRING());

		return psc;
	}

	private Object2ObjectOpenHashMap<String, ObjectList<Row>> buildIndex(Iterator<Row> it) {
		Object2ObjectOpenHashMap<String, ObjectList<Row>> map = new Object2ObjectOpenHashMap<>();
		while (it.hasNext()) {
			Row row = it.next();
			if (!map.containsKey(row.getString(0))) {
				map.put(row.getString(0), new ObjectArrayList<Row>());
			}
			map.get(row.getString(0)).add(row);
		}
		return map;
	}

}
