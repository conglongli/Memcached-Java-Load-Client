/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.ChurnGenerator;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.memcached.Memcached;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD
 * operations. The relative proportion of different kinds of operations, and
 * other properties of the workload, are controlled by parameters specified at
 * runtime.
 * 
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldcount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one
 * (false) (default: true)
 * <LI><b>writeallfields</b>: should updates and read/modify/writes update all
 * fields (true) or just one (false) (default: false)
 * <LI><b>readproportion</b>: what proportion of operations should be reads
 * (default: 0.95)
 * <LI><b>updateproportion</b>: what proportion of operations should be updates
 * (default: 0.05)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts
 * (default: 0)
 * <LI><b>scanproportion</b>: what proportion of operations should be scans
 * (default: 0)
 * <LI><b>readmodifywriteproportion</b>: what proportion of operations should be
 * read a record, modify it, write it back (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select
 * the records to operate on - uniform, zipfian or latest (default: uniform)
 * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to
 * scan (default: 1000)
 * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be
 * used to choose the number of records to scan, for each scan, between 1 and
 * maxscanlength (default: uniform)
 * <LI><b>insertorder</b>: should records be inserted in order by key
 * ("ordered"), or in hashed order ("hashed") (default: hashed)
 * </ul>
 */
public class MemcachedCoreWorkload extends Workload {

	IntegerGenerator keysequence;

	DiscreteGenerator operationchooser;

	IntegerGenerator keychooser;

	Generator fieldchooser;

	CounterGenerator transactioninsertkeysequence;

	IntegerGenerator scanlength;
	
	DiscreteGenerator costchooser;
	
	UniformIntegerGenerator highcostchooser;
	
	UniformIntegerGenerator midcostchooser;
	
	UniformIntegerGenerator lowcostchooser;

	boolean orderedinserts;

	/**
	 * Initialize the scenario. Called once, in the main client thread, before
	 * any operations are started.
	 */
	public void init() throws WorkloadException {		
		int recordcount = Config.getConfig().record_count;
		int insertstart = Config.getConfig().insert_start;
		
		if (Config.getConfig().insert_order.compareTo("hashed") == 0) {
			orderedinserts = false;
		} else {
			orderedinserts = true;
		}

		keysequence = new CounterGenerator(insertstart);
		operationchooser = new DiscreteGenerator();
		costchooser = new DiscreteGenerator();
		highcostchooser = new UniformIntegerGenerator(Config.getConfig().high_cost_min,
							Config.getConfig().high_cost_max);
		midcostchooser = new UniformIntegerGenerator(Config.getConfig().mid_cost_min,
							Config.getConfig().mid_cost_max);
		lowcostchooser = new UniformIntegerGenerator(Config.getConfig().low_cost_min,
							Config.getConfig().low_cost_max);
		
		if (Config.getConfig().memadd_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memadd_proportion, "ADD");
		}
		if (Config.getConfig().memappend_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memappend_proportion, "APPEND");
		}
		if (Config.getConfig().memcas_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memcas_proportion, "CAS");
		}
		if (Config.getConfig().memdecr_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memdecr_proportion, "DECR");
		}
		if (Config.getConfig().memdelete_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memdelete_proportion, "DELETE");
		}
		if (Config.getConfig().memget_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memget_proportion, "GET");
		}
		if (Config.getConfig().memgets_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memgets_proportion, "GETS");
		}
		if (Config.getConfig().memincr_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memincr_proportion, "INCR");
		}
		if (Config.getConfig().memprepend_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memprepend_proportion, "PREPEND");
		}
		if (Config.getConfig().memreplace_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memreplace_proportion, "REPLACE");
		}
		if (Config.getConfig().memset_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memset_proportion, "SET");
		}
		if (Config.getConfig().memupdate_proportion > 0) {
			operationchooser.addValue(Config.getConfig().memupdate_proportion, "UPDATE");
		}
		
		if (Config.getConfig().high_cost_prob > 0) {
			costchooser.addValue(Config.getConfig().high_cost_prob, "HIGH");
		}
		if (Config.getConfig().mid_cost_prob > 0) {
			costchooser.addValue(Config.getConfig().mid_cost_prob, "MID");
		}
		if (Config.getConfig().low_cost_prob > 0) {
			costchooser.addValue(Config.getConfig().low_cost_prob, "LOW");
		}
		
		transactioninsertkeysequence = new CounterGenerator(recordcount);
		if (Config.getConfig().request_distribution.compareTo("uniform") == 0) {
			keychooser = new UniformIntegerGenerator(0, recordcount - 1);
		} else if (Config.getConfig().request_distribution.compareTo("zipfian") == 0) {
			// it does this by generating a random "next key" in part by taking
			// the modulus over the number of keys
			// if the number of keys changes, this would shift the modulus, and
			// we don't want that to change which keys are popular
			// so we'll actually construct the scrambled zipfian generator with
			// a keyspace that is larger than exists at the beginning
			// of the test. that is, we'll predict the number of inserts, and
			// tell the scrambled zipfian generator the number of existing keys
			// plus the number of predicted keys as the total keyspace. then, if
			// the generator picks a key that hasn't been inserted yet, will
			// just ignore it and pick another key. this way, the size of the
			// keyspace doesn't change from the perspective of the scrambled
			// zipfian generator

			int opcount = Config.getConfig().operation_count;
			int expectednewkeys = (int) (((double) opcount) * Config.getConfig().memset_proportion * 2.0); // 2 is fudge factor
			keychooser = new ScrambledZipfianGenerator(recordcount + expectednewkeys);
		} else if (Config.getConfig().request_distribution.compareTo("latest") == 0) {
			keychooser = new SkewedLatestGenerator(transactioninsertkeysequence);
		} else if (Config.getConfig().request_distribution.compareTo("churn") == 0){
			keychooser = new ChurnGenerator(Config.getConfig().working_set, Config.getConfig().churn_delta, recordcount);
		} else {
			throw new WorkloadException("Unknown distribution \"" + Config.getConfig().request_distribution + "\"");
		}

		fieldchooser = new UniformIntegerGenerator(0, Config.getConfig().field_count - 1);

		if (Config.getConfig().scan_length_distribution.compareTo("uniform") == 0) {
			scanlength = new UniformIntegerGenerator(1, Config.getConfig().max_scan_length);
		} else if (Config.getConfig().scan_length_distribution.compareTo("zipfian") == 0) {
			scanlength = new ZipfianGenerator(1, Config.getConfig().max_scan_length);
		} else {
			throw new WorkloadException("Distribution \"" + Config.getConfig().scan_length_distribution + "\" not allowed for scan length");
		}
	}

	/**
	 * Do one insert operation. Because it will be called concurrently from
	 * multiple client threads, this function must be thread safe. However,
	 * avoid synchronized, or the threads will block waiting for each other, and
	 * it will be difficult to reach the target throughput. Ideally, this
	 * function would have no side effects other than DB operations.
	 */
	public ReturnMsg doInsert(DataStore memcached, int load) {
		ReturnMsg result_msg;
		int result;
		String value = null;
		int keynum = keysequence.nextInt();
		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		
		String dbkey = Config.getConfig().key_prefix + keynum;
		dbkey = dbkey.substring(dbkey.length() - 16 ,dbkey.length());
		//String value = Utils.ASCIIString(Config.getConfig().value_length);
		Integer cost = 0;
		String costl = costchooser.nextString();
		if (costl.compareTo("HIGH") == 0) {
			cost = highcostchooser.nextInt();
			value = Utils.ASCIIString(Config.getConfig().high_value_length);
		} else if (costl.compareTo("MID") == 0) {
			cost = midcostchooser.nextInt();
			value = Utils.ASCIIString(Config.getConfig().mid_value_length);
		} else if (costl.compareTo("LOW") == 0) {
			cost = lowcostchooser.nextInt();
			value = Utils.ASCIIString(Config.getConfig().low_value_length);
		}
		
		if (Config.getConfig().default_set == true) {
			result = ((Memcached)memcached).set(dbkey, value, load);
		} else {
			result = ((Memcached)memcached).set_cost(dbkey, value, load, (int)cost);
		}
		
		if (result == 0) {
			result_msg = new ReturnMsg(true, "SET", dbkey, cost, false);
			return result_msg;
		} else {
			result_msg = new ReturnMsg(false, "SET", dbkey, cost, false);
			return result_msg;
		}
	}

	/**
	 * Do one transaction operation. Because it will be called concurrently from
	 * multiple client threads, this function must be thread safe. However,
	 * avoid synchronized, or the threads will block waiting for each other, and
	 * it will be difficult to reach the target throughput. Ideally, this
	 * function would have no side effects other than DB operations.
	 */
	public ReturnMsg doTransaction(DataStore memcached, int num_set) {
		String op = operationchooser.nextString();
		ReturnMsg result;
		if (op.compareTo("ADD") == 0) {
			doTransactionAdd((Memcached)memcached);
		} else if (op.compareTo("APPEND") == 0) {
			doTransactionAppend((Memcached)memcached);
		} else if (op.compareTo("CAS") == 0) {
			doTransactionCas((Memcached)memcached);
		} else if (op.compareTo("DECR") == 0) {
			doTransactionDecr((Memcached)memcached);
		} else if (op.compareTo("DELETE") == 0) {
			doTransactionDelete((Memcached)memcached);
		} else if (op.compareTo("GET") == 0) {
			return doTransactionGet((Memcached)memcached);
		} else if (op.compareTo("GETS") == 0) {
			doTransactionGets((Memcached)memcached);
		} else if (op.compareTo("INCR") == 0) {
			doTransactionIncr((Memcached)memcached);
		} else if (op.compareTo("PREPEND") == 0) {
			doTransactionPrepend((Memcached)memcached);
		} else if (op.compareTo("REPLACE") == 0) {
			doTransactionReplace((Memcached)memcached);
		} else if (op.compareTo("SET") == 0) {
			return doInsert((Memcached)memcached, 0);
		} else if (op.compareTo("UPDATE") == 0) {
			doTransactionUpdate((Memcached)memcached);
		}
		/*if ((double)num_set/(double)(Config.getConfig().operation_count - 
		    Config.getConfig().record_count) > Config.getConfig().memset_proportion) {
			return doTransactionGet((Memcached)memcached);
		} else {
			return doInsert((Memcached)memcached);
		}*/
		result = new ReturnMsg(true, null, null, null, false);
		return result;
	}
	
	public void doTransactionAdd(Memcached memcached) {
		// choose the next key
		int keynum = transactioninsertkeysequence.nextInt();
		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String dbkey = Config.getConfig().key_prefix + keynum;
		String value = Utils.ASCIIString(Config.getConfig().value_length);
		memcached.add(dbkey, value);
	}
	
	public void doTransactionAppend(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String key = Config.getConfig().key_prefix + keynum;
		memcached.append(key, 0, "appended_string");
	}
	
	public void doTransactionCas(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String key = Config.getConfig().key_prefix + keynum;
		long cas = memcached.gets(key);
		String value = Utils.ASCIIString(Config.getConfig().value_length);
		memcached.cas(key, cas, value);
	}
	
	public void doTransactionDecr(Memcached memcached) {
		
	}
	
	public void doTransactionDelete(Memcached memcached) {
		
	}

	public ReturnMsg doTransactionGet(Memcached memcached) {
		int keynum;
		ReturnMsg result_msg;
		int result;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String keyname = Config.getConfig().key_prefix + keynum;
		keyname = keyname.substring(keyname.length() - 16 ,keyname.length());

		if (memcached.get(keyname, null) != 0) {
			//String value = Utils.ASCIIString(Config.getConfig().value_length);
			Integer cost = 0;
			String value = null;
			String costl = costchooser.nextString();
			if (costl.compareTo("HIGH") == 0) {
				cost = highcostchooser.nextInt();
				value = Utils.ASCIIString(Config.getConfig().high_value_length);
			} else if (costl.compareTo("MID") == 0) {
				cost = midcostchooser.nextInt();
				value = Utils.ASCIIString(Config.getConfig().mid_value_length);
			} else if (costl.compareTo("LOW") == 0) {
				cost = lowcostchooser.nextInt();
				value = Utils.ASCIIString(Config.getConfig().low_value_length);
			}
			
			if (Config.getConfig().default_set == true) {
				result = ((Memcached)memcached).set(keyname, value, 0);
			} else {
				result = ((Memcached)memcached).set_cost(keyname, value, 0, (int)cost);
			}
		
			if (result == 0) {
				result_msg = new ReturnMsg(true, "GET", keyname, cost, true);
				return result_msg;
			} else {
				result_msg = new ReturnMsg(false, "GET", keyname, cost, true);
				return result_msg;
			}
		}
		result_msg = new ReturnMsg(true, "GET", keyname, null, false);
		return result_msg;
	}
	
	public long doTransactionGets(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		return memcached.gets(Config.getConfig().key_prefix + keynum);
	}
	
	public void doTransactionIncr(Memcached memcached) {
		
	}
	
	public void doTransactionPrepend(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String key = Config.getConfig().key_prefix + keynum;
		memcached.prepend(key, 0, "prepended_string");
	}
	
	public void doTransactionReplace(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String key = Config.getConfig().key_prefix + keynum;
		String value = Utils.ASCIIString(Config.getConfig().value_length);
		memcached.replace(key, value);
	}
	
	public void doTransactionSet(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String keyname = Config.getConfig().key_prefix + keynum;
		String value = Utils.ASCIIString(Config.getConfig().value_length);
		memcached.set(keyname, value, 0);
	}
	
	public void doTransactionUpdate(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String keyname = Config.getConfig().key_prefix + keynum;
		String value = Utils.ASCIIString(Config.getConfig().value_length);
		memcached.update(keyname, value);
	}
}
