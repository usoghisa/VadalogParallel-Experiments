package prometheuxresearch.benchmark.vldb.experiment.scenarios.sameGeneration;

import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

/**
 * 
 * Copyright (C) Prometheux Limited. All rights reserved.
 * 
 * @author Prometheux Limited
 */

public class SameGenerationFlatMapGroup implements FlatMapGroupsFunction<Row, Row, Row> {

	private static final long serialVersionUID = 5277710114026239859L;

	private Broadcast<Object2ObjectOpenHashMap<Integer, ObjectOpenHashSet<Row>>> arc;

	public SameGenerationFlatMapGroup(Broadcast<Object2ObjectOpenHashMap<Integer, ObjectOpenHashSet<Row>>> bVar) {
		this.arc = bVar;
	}

	@Override
	public Iterator<Row> call(Row key, Iterator<Row> it) throws Exception {

		ObjectOpenHashSet<Row> allSg = new ObjectOpenHashSet<>();
		ObjectOpenHashSet<Row> deltaSg = new ObjectOpenHashSet<>();

		// linear rule
		while (it.hasNext()) {
			Row currentArc = it.next();
			Integer p = currentArc.getInt(0);
			Integer x = currentArc.getInt(1);
			if (this.arc.getValue().containsKey(p)) {
				ObjectOpenHashSet<Row> matchingRight = this.arc.getValue().get(p);
				for (Row arcRhs : matchingRight) {
					Integer y = arcRhs.getInt(1);
					if (!x.equals(y)) {
						Row newRow = RowFactory.create(x, y);
						if (!allSg.contains(newRow)) {
							allSg.add(newRow);
							deltaSg.add(newRow);
						}
					}
				}
			}

		}

		ObjectOpenHashSet<Row> newDelta = new ObjectOpenHashSet<>();
		while (!deltaSg.isEmpty()) {
			Iterator<Row> iterator = deltaSg.iterator();
			while (iterator.hasNext()) {
				Row currentSg = iterator.next();
				Integer a = currentSg.getInt(0);
				Integer b = currentSg.getInt(1);
				if (this.arc.getValue().containsKey(a) && this.arc.getValue().containsKey(b)) {
					ObjectOpenHashSet<Row> matching1 = this.arc.getValue().get(a);
					ObjectOpenHashSet<Row> matching2 = this.arc.getValue().get(b);
					for (Row row1 : matching1) {
						for (Row row2 : matching2) {
							Integer x = row1.getInt(1);
							Integer y = row2.getInt(1);
							Row newRow = RowFactory.create(x, y);
							if (!allSg.contains(newRow)) {
								allSg.add(newRow);
								newDelta.add(newRow);
							}

						}
					}

				}
			}
			deltaSg = newDelta;
			newDelta = new ObjectOpenHashSet<>();
		}

		return allSg.iterator();

	}

}
