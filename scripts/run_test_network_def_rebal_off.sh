#!/bin/bash

# Usage: ./run_test_network.sh [-g] [-i numiter] filename1, filename2,...
# This is the main script for running the tests. It starts the memcached server on sp08, and runs memaslap on local server. If [-g] is given the gds_replace memcached server is started on sp08. If -i is given, the configurations are run numiter times each. The filenames provided (filename1, filename2,...) are the config files to be run. If no filenames are given, all the config files in ../config/ are used.

# Change to script directory
cd $(dirname $0)

CONFIG_DIR="../workloads_rebal_off"

OUTPUT_DIR="../output"

RUN_TIME=10000

LIB_DIR="../lib"

YCSB_DIR="/home/cl19/YCSB/Memcached-Java-Load-Client"

USE_TOP=1

NUM_ITERATIONS=1

if [ ! -d $OUTPUT_DIR ]; then
    mkdir $OUTPUT_DIR
fi

useConfigDir=false

# if using gds_replace memcached, set to '-g'
USE_GDS=

if [ ! $# -eq 0 ]; then
    while getopts "gi:" opt; do
        case $opt in
            g)
                USE_GDS="-g"
                ;;
            i)
                NUM_ITERATIONS=$OPTARG
                ;;
            \?)
                echo "Invalid option: -$OPTARG"
                exit 1
                ;;
        esac
    done
    shift $((OPTIND-1))
fi

if [ ! $# -eq 0 ]; then
    config_files=$@
else
    if [ ! -d $CONFIG_DIR ]; then
        echo "Default config directory not found"
        exit 1
    fi
    config_files=$(ls $CONFIG_DIR)
    useConfigDir=true
fi

echo $config_files

mem=10240
ssh cl19@sp08.cs.rice.edu "bash scripts/memcached_server.sh $USE_GDS -m $mem" 
for (( i = 1; i <= $NUM_ITERATIONS; i++ ));
do
    for config_file in $config_files
    do  
        if $useConfigDir ; then
            filename=${CONFIG_DIR}/${config_file}
            outputname="${config_file}_$i"
        else
            filename=$config_file
            outputname="${filename}_$i"
        fi

        if [ ! -z $USE_GDS ]; then
            outputname="${outputname}_gdsreplace"
        fi

        #mem=12288

        # Start memcached server
        #ssh cl19@sp08.cs.rice.edu "bash scripts/memcached_server.sh $USE_GDS -m $mem" 

        echo $config_file > $OUTPUT_DIR/${outputname}.output
        if [ -z $USE_GDS ]; then
            echo "Using default memcached..." >> $OUTPUT_DIR/${outputname}.output
        else
            echo "Using gds_replace memcached..." >> $OUTPUT_DIR/${outputname}.output
        fi

		# Start YCSB
		java -cp $YCSB_DIR/build/ycsb.jar:$LIB_DIR/spymemcached-2.9.0.jar:$LIB_DIR/jackson-core-asl-1.5.2.jar:$LIB_DIR/jackson-mapper-asl-1.5.2.jar:$LIB_DIR/slf4j-api-1.6.1.jar:$LIB_DIR/slf4j-simple-1.6.1.jar com.yahoo.ycsb.LoadGenerator -t -P $filename >> $OUTPUT_DIR/${outputname}.output
		#if [ $USE_TOP -eq 1 ]; then
            # start cpu script on server
            #ssh cl19@sp08.cs.rice.edu "nohup scripts/cpu.sh -t $RUN_TIME > scripts/output/${outputname}_top_output 2> cpu.err < /dev/null &"
                
            # start cpu script on client
            #./cpu.sh -t $RUN_TIME > $OUTPUT_DIR/${outputname}_client_top_output
                
            #sleep 5   # make sure memaslap finishes before continuing
                
            # download output to client
            #scp cl19@sp08.cs.rice.edu:scripts/output/${outputname}_top_output $OUTPUT_DIR/${outputname}_server_top_output
        #fi
		# Get memcached stats
            #echo "stats" | nc -q 3 sp08.cs.rice.edu 11211 > $OUTPUT_DIR/${outputname}_server_stats
            #echo "stats slabs" | nc -q 3 sp08.cs.rice.edu 11211 > $OUTPUT_DIR/${outputname}_server_stats_slabs
    done
done

ssh cl19@sp08.cs.rice.edu 'pkill memcached'
