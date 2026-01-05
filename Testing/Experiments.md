# Experiments

To recreate the experiments the java LoadGenerator as well as the KafkaLagPlotterV2 and the KafkaLagAverageStdPlotter applications are required.
Assuming the Docker containers are running and the SQL Tables have been created and the checkpointing was set and the INSERTS were posted as a single job with a Statement Set, then the experiments can be recreated as follows:


## Experiment 1

### Prerequisites

- Within the java LoadGenerator the rate argument (line 44) can be adjusted to the desired one. 
- Within the KafkaLagPlotterV2 the name of the files must be changed each iteration to the corresponding number (lines 19, 20)
- For the first run the directory must be "kafka_lag_run1" for the second "kafka_lag_run2" etc
- The naming is important since the KafkaLagAveragePlotter is based on the naming to draw the averages and standard deviation
- The csv and png files will be created in the same folder as KafkaLagPlotterV2
- The KafkaLagAveragePlotter must be in the same directory as the files
- The Flink UI must be opened. Within the Job, in the first task "source: cart_events", in the Metrics tab, the Metrics numRecordsOutPerSecond is selected and switched to numeric. This is the throughput metric. Adjust the Graph so the Backpressure is visible.


### Execution

The java LoadGenerator and the KafkaLagPlotter are started at the same time. An independant stopwatch is also started simultanously. The Backpressure and the throughput are observed and the maximum is noted and updated as the UI updates arrive. When the LoadGenerator stops a lap is also stopped with the stopwatch and the time Tstop is noted. When the backpressure reaches 0 another lap is stopped with the stopwatch and the time Tbz is noted. When then KafkaLagPlotter displays the first zero, a last lap is stopped and the time Tkz is noted. The time Tdrain is calculated as "MAX(Tbz, Tkz) - Tstop". This is done automatically if using the excel template provided in [Exp1Table](../Testing/Experiment1/Exp1Table.xlsx). 

The process is repeated 10 times, each time restarting the cluster and kafka topics before the execution. For each run, the file names in KafkaLagPlotterV2 must be adjusted. The mean and the std are calculated over the 10 runs. This is also done automatically in the Excel File.

After the 10 runs are completed. The KafkaLagAverageStdPlotter can be started in the same directory as the csv files. This plots the average along with the first standard deviation.

This is also repeated for different load scenarions (events / s). This is adjusted within the LoadGenerator


## Experiment 2

### Prerequisites


### Execution

