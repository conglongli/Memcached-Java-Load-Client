package com.yahoo.ycsb.client;

import com.yahoo.ycsb.Config;
import com.yahoo.ycsb.DataStore;
import com.yahoo.ycsb.DataStoreException;
import com.yahoo.ycsb.ReturnMsg;
import com.yahoo.ycsb.UnknownDataStoreException;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.database.DBFactory;
import com.yahoo.ycsb.memcached.MemcachedFactory;

import java.util.Hashtable;

/**
 * A thread pool is a group of a limited number of threads that are used to
 * execute tasks.
 */
public class ClientThreadPool extends ThreadGroup {
	private boolean isAlive;
	private int threadID;
	private int ops;
	private int total_miss_cost;
	private int total_miss;
	private int num_get;
	private int num_set;
	private static int threadPoolID;
	private Hashtable<String, Integer> costs;

	public ClientThreadPool(int numThreads, int ops, Workload workload) {
		super("ThreadPool-" + (threadPoolID++));
		this.ops = ops;
		setDaemon(true);

		isAlive = true;
		
		total_miss_cost = 0;
		total_miss = 0;
		num_get = 0;
		num_set = 0;
		costs = new Hashtable<String, Integer>();
		
		for (int i = 0; i < numThreads; i++) {
			DataStore db = null;
			try {
				if (workload instanceof com.yahoo.ycsb.workloads.MemcachedCoreWorkload)
					db = MemcachedFactory.newMemcached(Config.getConfig().db);
				else if (workload instanceof com.yahoo.ycsb.workloads.DBCoreWorkload)
					db = DBFactory.newDB(Config.getConfig().db);
				else {
					System.out.println("Invalid Database/Workload Combination");
					System.exit(0);
				}
				db.init();
			} catch (UnknownDataStoreException e) {
				System.out.println("Unknown DataStore " + Config.getConfig().db);
				System.exit(0);
			} catch (DataStoreException e) {
				e.printStackTrace();
				System.exit(0);
			}
			new PooledThread(workload, db).start();
		}
	}

	protected synchronized boolean getTask() {
		if (!isAlive || ops <= 0)
			return false;
		ops--;
		return true;
	}

	public synchronized void close() {
		if (isAlive) {
			isAlive = false;
			interrupt();
		}
	}
	
	public synchronized void processResult(ReturnMsg returnMsg) {
		if (returnMsg.op.compareTo("SET") == 0) {
			costs.put(returnMsg.dbkey, returnMsg.cost);
			num_set++;
		} else if (returnMsg.op.compareTo("GET") == 0) {
			if (returnMsg.miss == true) {
				Integer value = costs.get(returnMsg.dbkey);
				if (value == null) {
					total_miss_cost += returnMsg.cost;
				} else {
					total_miss_cost += value;
				}
				total_miss++;
				num_set++;
				costs.put(returnMsg.dbkey, returnMsg.cost);
				ops--;
			}
			num_get++;
		}
	}

	/**
	 * Closes this ThreadPool and waits for all running threads to finish. Any
	 * waiting tasks are executed.
	 */
	public void join() {
		// notify all waiting threads that this ThreadPool is no
		// longer alive
		/*synchronized (this) {
			isAlive = false;
			notifyAll();
		}*/

		// wait for all threads to finish
		Thread[] threads = new Thread[activeCount()];
		int count = enumerate(threads);
		for (int i = 0; i < count; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException ex) {
			}
		}
	}

	/**
	 * A PooledThread is a Thread in a ThreadPool group, designed to run tasks
	 * (Runnables).
	 */
	private class PooledThread extends Thread {
		private Workload workload;
		private DataStore db;
		
		public PooledThread(Workload workload, DataStore db) {
			super(ClientThreadPool.this, "PooledThread-" + (threadID++));
			this.workload = workload;
			this.db = db;
		}

		public void run() {
			while (!isInterrupted() && getTask()) {
				/*if (Config.getConfig().do_transactions) {
					workload.doTransaction(db);
				} else {
					workload.doInsert(db);
				}*/
				if (Config.getConfig().operation_count - ops > Config.getConfig().record_count) {
					ReturnMsg result = workload.doTransaction(db);
					processResult(result);
				} else {
					ReturnMsg result = workload.doInsert(db);
					processResult(result);
				}
			}
			
			// TODO: Probably shouldn't be here
			try {
				db.cleanup();
			} catch (DataStoreException e) {
				e.printStackTrace();
			}
			
			System.out.println("Client Thread Done. Total Miss Cost = " + total_miss_cost 
			+ " Total Miss = " + total_miss + " Num Get = " + num_get + " Num Set = " + num_set);
		}
	}
}
