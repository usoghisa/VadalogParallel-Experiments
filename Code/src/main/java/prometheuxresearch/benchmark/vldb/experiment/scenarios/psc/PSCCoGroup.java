package prometheuxresearch.benchmark.vldb.experiment.scenarios.psc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.shaded.org.apache.commons.codec.digest.DigestUtils;
import org.apache.spark.api.java.function.CoGroupFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

/**
 * 
 * Copyright (C) Prometheux Limited. All rights reserved.
 * 
 * @author Prometheux Limited
 */

public class PSCCoGroup implements CoGroupFunction<Row, Row, Row, String> {

	private static final long serialVersionUID = 1365579740825908889L;

	private Broadcast<Object2ObjectOpenHashMap<String, ObjectList<Row>>> bControl;
	private Broadcast<Object2ObjectOpenHashMap<String, ObjectList<Row>>> bPerson;
	private int labelledNullIndex = 0;

	public PSCCoGroup(Broadcast<Object2ObjectOpenHashMap<String, ObjectList<Row>>> bControl,
			Broadcast<Object2ObjectOpenHashMap<String, ObjectList<Row>>> bPerson) {
		super();
		this.bControl = bControl;
		this.bPerson = bPerson;
	}

	@Override
	public Iterator<String> call(Row key, Iterator<Row> keyPersonIt, Iterator<Row> companyIt) throws Exception {
		ObjectOpenHashSet<String> psc = new ObjectOpenHashSet<>();
		ObjectArrayList<Row> deltaPsc = new ObjectArrayList<Row>();
		String nullSeed = key.get(0).toString();

		while (keyPersonIt.hasNext()) {
			Row currRow = keyPersonIt.next();
			String joinKey = currRow.getString(1);
			if (this.bPerson.value().containsKey(joinKey)) {
				Row toGenerate = RowFactory.create(currRow.getString(0), currRow.getString(0), currRow.getString(1));
				String reasoningKey = this.getReasoningKey(this.extractArguments(toGenerate));
				if (!psc.contains(reasoningKey)) {
					psc.add(reasoningKey);
					deltaPsc.add(toGenerate);
				}

			}
		}

		while (companyIt.hasNext()) {
			Row currRow = companyIt.next();
			this.labelledNullIndex++;
			String nullValue = "z" + "_" + nullSeed + "_" + this.labelledNullIndex;
			Row toGenerate = RowFactory.create(currRow.getString(0), currRow.getString(0), nullValue);
			String reasoningKey = this.getReasoningKey(this.extractArguments(toGenerate));
			if (!psc.contains(reasoningKey)) {
				psc.add(reasoningKey);
				deltaPsc.add(toGenerate);
			}
		}

		ObjectArrayList<Row> newDelta = new ObjectArrayList<Row>();
		while (!deltaPsc.isEmpty()) {
			Iterator<Row> it = deltaPsc.iterator();
			while (it.hasNext()) {
				Row currPsc = it.next();
				String joinKey = currPsc.getString(0);
				if (this.bControl.value().containsKey(joinKey)) {
					for (Row control : this.bControl.value().get(joinKey)) {
						Row pscToGenerate = RowFactory.create(control.getString(1), currPsc.getString(1),
								currPsc.getString(1));
						String reasoningKey = this.getReasoningKey(this.extractArguments(pscToGenerate));
						if (!psc.contains(reasoningKey)) {
							psc.add(reasoningKey);
							newDelta.add(pscToGenerate);
						}
					}
				}
			}

			deltaPsc = newDelta;
			newDelta = new ObjectArrayList<Row>();
		}

		return psc.iterator();
	}

	private List<Object> extractArguments(Row row) {
		List<Object> args = new ArrayList<>();
		for (int i = 0; i < row.length(); i++)
			args.add(row.get(i));
		return args;
	}

	private String getReasoningKey(List<Object> keyArguments) {
		Map<Object, Integer> nullToIndex = new HashMap<>();
		Integer ind = 0;
		List<Object> renamedValues = new ArrayList<>();
		for (Object arg : keyArguments) {
			if (arg instanceof String && ((String) arg).startsWith("z_")) {
				if (!nullToIndex.containsKey(arg)) {
					nullToIndex.put(arg, ind);
					ind++;
				}
				renamedValues.add(nullToIndex.get(arg));
			} else {
				renamedValues.add(arg);
			}
		}
		return DigestUtils.md5Hex(renamedValues.toString());
	}

}
