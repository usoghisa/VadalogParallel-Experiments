package prometheuxresearch.benchmark.vldb.experiment.flink.psc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.hadoop.shaded.org.apache.commons.codec.digest.DigestUtils;

/**
 * 
 * Copyright (C) Prometheux Limited. All rights reserved.
 * 
 * @author Prometheux Limited
 */

public class ReasoningKeyMapper2 implements MapFunction<Tuple1<String>, Tuple4<String, String, String, String>> {

	private static final long serialVersionUID = 5805769737759616534L;

	@Override
	public Tuple4<String, String, String, String> map(Tuple1<String> t) throws Exception {
		String labelledNull = "z_" + DigestUtils.md5Hex(Arrays.asList(t.f0, t.f0).toString()) + "_2";
		List<Object> keyArguments = Arrays.asList(t.f0, t.f0, labelledNull);
		return new Tuple4<String, String, String, String>(t.f0, t.f0, labelledNull, getReasoningKey(keyArguments));
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
