package prometheuxresearch.benchmark.vldb.experiment.scenarios.transitiveClosure;

import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashBigSet;

/**
 * 
 * Copyright (C) Prometheux Limited. All rights reserved.
 * 
 * @author Prometheux Limited
 */

public class TransitiveClosureFlatMapGroup implements FlatMapGroupsFunction<Row, Row, Row> {

	private static final long serialVersionUID = 9078121082040863488L;

	private Broadcast<Object2ObjectOpenHashMap<Integer, ObjectOpenHashBigSet<Row>>> map;

	public TransitiveClosureFlatMapGroup(Broadcast<Object2ObjectOpenHashMap<Integer, ObjectOpenHashBigSet<Row>>> bVar) {
		this.map = bVar;
	}

	@Override
	public Iterator<Row> call(Row k, Iterator<Row> it) throws Exception {
		ObjectOpenHashBigSet<Row> deltaPaths = new ObjectOpenHashBigSet<Row>(it);
		ObjectOpenHashBigSet<Row> allPaths = new ObjectOpenHashBigSet<>(deltaPaths);
		ObjectOpenHashBigSet<Row> newDelta = new ObjectOpenHashBigSet<>();

		while (!deltaPaths.isEmpty()) {
			Iterator<Row> iterator = deltaPaths.iterator();
			while (iterator.hasNext()) {
				Row row = iterator.next();
				Integer key = row.getInt(1);
				if (this.map.value().containsKey(key)) {
					ObjectOpenHashBigSet<Row> edges = this.map.value().get(key);
					for (Row edge : edges) {
						Row newPath = RowFactory.create(row.getInt(0), edge.getInt(1));
						if (!allPaths.contains(newPath)) {
							newDelta.add(newPath);
							allPaths.add(newPath);
						}
					}
				}
			}
			deltaPaths = newDelta;
			newDelta = new ObjectOpenHashBigSet<>();

		}
		return allPaths.iterator();
	}
}
