package prometheuxresearch.benchmark.vldb.experiment.scenarios.closeLink;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import scala.collection.JavaConverters;

/**
 * 
 * Copyright (C) Prometheux Limited. All rights reserved.
 * 
 * @author Prometheux Limited
 */

public class CloseLinkFlatMapGroup implements FlatMapGroupsFunction<Row, Row, Row> {

	private static final long serialVersionUID = 4826992040791771757L;

	private Broadcast<Object2ObjectOpenHashMap<Integer, ObjectList<Row>>> own;

	public CloseLinkFlatMapGroup(Broadcast<Object2ObjectOpenHashMap<Integer, ObjectList<Row>>> bVar) {
		this.own = bVar;
	}

	@Override
	public Iterator<Row> call(Row key, Iterator<Row> ownPart) throws Exception {

		ObjectArrayList<Row> newDeltaCloseLink = new ObjectArrayList<Row>();

		Object2ObjectOpenHashMap<Row, Double> aggregationTable = new Object2ObjectOpenHashMap<>();
		Object2ObjectOpenHashMap<Row, Double> contributorTable = new Object2ObjectOpenHashMap<>();

		Object2ObjectOpenHashMap<Row, ObjectOpenHashSet<Integer>> visitedNodes = new Object2ObjectOpenHashMap<>();

		ObjectArrayList<Row> deltaCloseLink = new ObjectArrayList<Row>();

		// direct close link / linear rule
		while (ownPart.hasNext()) {

			Row currentOwn = ownPart.next();
			// current control pair to evaluate
			Row group = RowFactory.create(currentOwn.get(0), currentOwn.get(1));
			Row contributors = RowFactory.create(currentOwn.get(0), currentOwn.get(1), currentOwn.get(1));

			// let us init the aggregation table if not present
			if (!contributorTable.containsKey(contributors)) {
				contributorTable.put(contributors, 0.0);
			}
			// let us init the aggregation table if not present
			if (!aggregationTable.containsKey(group)) {
				aggregationTable.put(group, 0.0);
			}

			// let us init the visited nodes if not present
			if (!visitedNodes.containsKey(group)) {
				visitedNodes.put(group, new ObjectOpenHashSet<>());
			}

			// visited nodes for the current pair
			ObjectOpenHashSet<Integer> visited = visitedNodes.get(group);
			visited.add(currentOwn.getInt(0));
			visited.add(currentOwn.getInt(1));

			Double newWeight = currentOwn.getDouble(2);
			Double contributorWeight = contributorTable.get(contributors);
			if (newWeight > contributorWeight) {
				contributorTable.replace(contributors, newWeight);
				Double oldWeight = aggregationTable.get(group);
				Double actualWeight = oldWeight - contributorWeight + newWeight;
				aggregationTable.replace(group, actualWeight);
				Row closeLinkRow = RowFactory.create(group.get(0), group.get(1), actualWeight,
						JavaConverters.asScalaBuffer((List<Integer>) new ArrayList<Integer>(visited)));
				deltaCloseLink.add(closeLinkRow);
			}
		}

		// indirect control
		while (!deltaCloseLink.isEmpty()) {
			Iterator<Row> iterator = deltaCloseLink.iterator();
			while (iterator.hasNext()) {
				// join left hand side, control pair
				Row closeLink = iterator.next();
				Integer Y = closeLink.getInt(1);
				// if there is a join match with the ownership values
				if (this.own.value().containsKey(Y)) {
					ObjectList<Row> ownershipEdges = this.own.value().get(Y);
					// current ownership edge
					for (Row ownershipEdge : ownershipEdges) {

						Integer X = closeLink.getInt(0);
						Integer Z = ownershipEdge.getInt(1);
						Double W1 = closeLink.getDouble(2);
						List<Integer> visited = closeLink.getList(3);
						Double W2 = ownershipEdge.getDouble(2);

						if (!visited.contains(Z)) {
							Double W = W1 * W2;
							// (X,Z)
							Row group = RowFactory.create(X, Z);
							if (!visitedNodes.containsKey(group)) {
								visitedNodes.put(group, new ObjectOpenHashSet<>());
							}

							ObjectOpenHashSet<Integer> newVisited = visitedNodes.get(group);
							newVisited.addAll(visited);
							newVisited.add(Z);

							if (!aggregationTable.containsKey(group)) {
								aggregationTable.put(group, 0.0);
							}

							Row contributors = RowFactory.create(X, Y, Z);
							if (!contributorTable.containsKey(contributors)) {
								contributorTable.put(contributors, 0.0);
							}
							Double oldContributor = contributorTable.get(contributors);
							if (oldContributor < W) {
								Double oldSum = aggregationTable.get(group);
								contributorTable.replace(contributors, W);
								double newWeight = oldSum + W - oldContributor;
								aggregationTable.replace(group, newWeight);
								Row closeLinkRow = RowFactory.create(group.get(0), group.get(1), newWeight,
										JavaConverters
												.asScalaBuffer((List<Integer>) new ArrayList<Integer>(newVisited)));
								deltaCloseLink.add(closeLinkRow);
							}

						}
					}

				}
			}
			deltaCloseLink = newDeltaCloseLink;
			newDeltaCloseLink = new ObjectArrayList<Row>();
		}

		return aggregationTable.entrySet().stream().filter(x -> x.getValue() >= 0.5).map(x -> x.getKey())
				.collect(Collectors.toList()).iterator();
	}
}