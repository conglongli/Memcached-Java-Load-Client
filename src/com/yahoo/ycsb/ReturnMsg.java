package com.yahoo.ycsb;

public class ReturnMsg {
	public boolean result;
	public String op;
    public String dbkey;
    public Integer cost;
    public boolean miss;
    
    public ReturnMsg(boolean result, String op, String dbkey, Integer cost, boolean miss) {
    	this.result = result;
    	this.op = op;
    	this.dbkey = dbkey;
    	this.cost = cost;
    	this.miss = miss;
    }
}