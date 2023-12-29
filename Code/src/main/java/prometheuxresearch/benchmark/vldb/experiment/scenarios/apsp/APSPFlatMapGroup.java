package prometheuxresearch.benchmark.vldb.experiment.scenarios.apsp;

import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

/**
 * 
 * Copyright (C) Prometheux Limited. All rights reserved.
 * 
 * @author Prometheux Limited
 */

public class APSPFlatMapGroup implements FlatMapGroupsFunction<Row, Row, Row> {

	private static final long serialVersionUID = 9213860881843120556L;

	private Broadcast<Object2ObjectOpenHashMap<Integer, ObjectList<Row>>> edge;

	public APSPFlatMapGroup(Broadcast<Object2ObjectOpenHashMap<Integer, ObjectList<Row>>> bVar) {
		this.edge = bVar;
	}

	@Override
	public Iterator<Row> call(Row key, Iterator<Row> edgePart) throws Exception {

		ObjectOpenHashSet<Row> deltaAPSP = new ObjectOpenHashSet<Row>();
		ObjectOpenHashSet<Row> newDeltaAPSP = new ObjectOpenHashSet<Row>();
		Object2ObjectOpenHashMap<Row, Double> APSP = new Object2ObjectOpenHashMap<>();

		// direct control
		while (edgePart.hasNext()) {

			Row currentPath = edgePart.next();
			Integer X = currentPath.getInt(0);
			Integer Y = currentPath.getInt(1);
			Double D = currentPath.getDouble(2);

			Row XY = RowFactory.create(X, Y);

			// if the current edge is not already visited for the current pair
//			if (!X.equals(Y)) {
				if (!APSP.containsKey(XY)) {
					APSP.put(XY, D);
					deltaAPSP.add(currentPath);
				} else {
					Double previousD = APSP.get(XY);
					if (previousD > D) {
						APSP.replace(XY, D);
						Row previousRow = RowFactory.create(X, Y, previousD);
						deltaAPSP.remove(previousRow);
						deltaAPSP.add(currentPath);
					}
				}
			}
//		}
		// indirect control
		while (!deltaAPSP.isEmpty()) {
			Iterator<Row> iterator = deltaAPSP.iterator();
			while (iterator.hasNext()) {
				Row APSPRow = iterator.next();
				Integer Y = APSPRow.getInt(1);

				if (this.edge.value().containsKey(Y)) {
					ObjectList<Row> nextEdges = this.edge.value().get(Y);
					for (Row nextEdge : nextEdges) {
						Integer X = APSPRow.getInt(0);
						Double D1 = APSPRow.getDouble(2);

						Integer Z = nextEdge.getInt(1);
						Double D2 = nextEdge.getDouble(2);
//						if (X != Z) {
							Row XZ = RowFactory.create(X, Z);
							Row currentSP = RowFactory.create(X, Z, D1 + D2);
							if (!APSP.containsKey(XZ)) {
								APSP.put(XZ, D1 + D2);
								newDeltaAPSP.add(currentSP);
							} else {
								Double previousD = APSP.get(XZ);
								if (previousD > D1 + D2) {
									APSP.replace(XZ, D1 + D2);
									newDeltaAPSP.add(currentSP);
								}
							}
//						}
					}
				}
			}
			deltaAPSP = newDeltaAPSP;
			newDeltaAPSP = new ObjectOpenHashSet<Row>();
		}

		return APSP.entrySet().stream().map(r -> RowFactory.create(r.getKey().getInt(0), r.getKey().getInt(1), r.getValue())).iterator();
	}
}
