# topology-change-simulator
In this repository, we have code to setup a distributed NebulaStream topology. This code enable us to simulate topological changes based on the provided configuration files.  

# Steps to launch simulation

1. Compile simulation
```
cargo build --release 
```
2. Command to start the simulation:
```
./target/release/start_experiment <STATEFUL/STATELESS> <nes directory> <experiment input config path> <output directory> <tcp input server executable> <number of runs> <log level (optional)>, <experiment path for retrial (optional)>
```
## Parameters 
<STATEFUL/STATELESS> - defines type of operator to experiment 

<nes directory> - directory with compiled NebulaStream system 

<experiment input config path> - path to the configuration file 

<output directory> - path to the directory to write experiment resutls 

<tcp input server executable> - path to the binary file of TCP input server 
	
<number of runs> - number of the runs of the experiment

# Example of configuration 

```
# This setting defines the reconfiguration mechanism to use
enable_query_reconfiguration = [true, false]

# defines how many tuples are contained in each batch that the data generator ingests into the system
tuples_per_buffer = [1]

# defines at what frequency new batches are ingested
gathering_interval = [10]

# adjust the speed at which the toplogy updates defined in topology_updates.json are applied (2 => half the speed, 0.5 => double the speed)
speedup_factor = [2, 1, 0.5]

# defines how many threads to use for placement computation. The single threaded setting is skipped if query reconfiguration is activated
placementAmendmentThreadCount = [8, 1]

# The following values will be used as long as they are not overridden by one of the lists defined above
[default_config.parameters]

# defines how many threads to use for placement computation.
placementAmendmentThreadCount = 8

# defines the time in secodns to wait after query deployment before starting to ingest data
deployment_time_offset = 160

# adjust the speed at which the toplogy updates defined in topology_updates.json are applied (2 => half the speed, 0.5 => double the speed)
speedup_factor = 1.0

# defines how many seconds after beginning to ingest data the first reconnects will occur
warmup = 20

reconnect_runtime = 120
cooldown_time = 10
post_cooldown_time = 60
enable_query_reconfiguration = true
enable_proactive_deployment = true
reconnect_input_type = "LIVE"
source_input_server_port = 9999

# Defines after how many tuples a tuple should be joined
join_match_interval = 7
window_size = 10

# The query to run
query_string = "Query::from(\"{INPUT1}\").joinWith(Query::from(\"{INPUT2}\")).where(Attribute(\"join_id\") == Attribute(\"join_id\")).window(TumblingWindow::of(EventTime(Attribute(\"value\")), Milliseconds({WINDOW_SIZE})))"
place_default_sources_on_node_ids_path = "<exp_dir>/source_groups.json"
num_worker_threads = 1

[default_config.default_source_input]
tuples_per_buffer = 0
gathering_interval = 0
source_input_method = "TCP"

[default_config.paths]
# Path to the file that contains the fixed topology
fixed_topology_nodes = "fixed_topology.json"

[default_config.paths.mobile_trajectories_directory]
TrajectoriesDir = "."
```