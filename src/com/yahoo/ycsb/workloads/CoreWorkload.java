package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.DataStore;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.ReturnMsg;

public abstract class CoreWorkload extends Workload{

	@Override
	public abstract ReturnMsg doInsert(DataStore db);

	@Override
	public abstract ReturnMsg doTransaction(DataStore db, int num_set);
	
	

}
