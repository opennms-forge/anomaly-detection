
package anomalydetector;

import java.util.List;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.InputStream;

import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
import com.yahoo.egads.control.DetectAnomalyProcessable;
import com.yahoo.egads.control.ProcessableObjectFactory;
import com.yahoo.egads.data.Anomaly;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.egads.utilities.*;
import java.io.File;


public class AnomalyDetector {
	
    public static void main(String[] args) throws Exception {
    	//System.setOut(new PrintStream(new FileOutputStream("output.txt")));
    	
    	//load egads properties from config file - determines time series model and anomaly detection model to use; and parameters
        Properties p = new Properties();
        String config = "config.ini";
        File f = new File(config);
        boolean isRegularFile = f.exists();
        if (isRegularFile) {
            InputStream is = new FileInputStream(config);
            p.load(is);
        } else {
        	FileUtils.initProperties(config, p);
        }
        
    	//get data from kafka consumer
    	String kafka_bootstrap_servers = "172.20.42.84:9092";
    	long pollingIntervalInMs = 10000L;
    	Consumer kafkaConsumer = new Consumer(kafka_bootstrap_servers, pollingIntervalInMs);
        
    	//This amount of ms is 1 day
    	long windowSizeMs = 86_400_000; //maximum ms difference between oldest timestamp and newest timestamp in metric
    	MetricsDatabase metricsDB = new MetricsDatabase(windowSizeMs);
    	
    	//create a simple database to store anomalies in by node & metric
    	AnomalyDatabase anomalyDB = new AnomalyDatabase();
    	
    	//create the graphing ui
    	//TODO untested
    	//GraphUI graph = new GraphUI();
    	
	    while(true) {
	    	System.out.println("New polling period...");

	    	//poll consumer for a given amount of time, populating the Metrics object with the latest measurements
	    	kafkaConsumer.updateMetricsFromKafka(metricsDB);
	        //debug output for metrics:
	    	System.out.print(metricsDB + "\n");
	    	
	    	//perform anomaly detection on each timeseries in the Metrics object
	    	List<MetricTimeSeries> timeSeriesList = metricsDB.asList();
	        //convert the time series data from kafka into the TimeSeries class EGADS uses
	        for(MetricTimeSeries metric : timeSeriesList) {
	        	System.out.println("Searching for anomalies in: " + metric.getSourceNode() + " | " + metric.getMetricName());
		        TimeSeries egadsTimeSeries = new TimeSeries(Longs.toArray(metric.getTimestamps()), Floats.toArray(metric.getValues()));
		        
		        //entry point for egads stuff
		        //build the time series model and anomaly detection model; check for anomalies
		        DetectAnomalyProcessable dap = (DetectAnomalyProcessable)ProcessableObjectFactory.create(egadsTimeSeries, p);
		        dap.process(); //creates models & runs anomaly detection
		        
		        //display output
		        List<Anomaly> result = dap.result(); //gets results from egads
		        for(Anomaly anom : result) {
		        	Anomaly.IntervalSequence anomalies = anom.intervals;
		        	System.out.println("# Anomalies: " + anomalies.size());
		        	for(Anomaly.Interval anomaly : anomalies) {
		        		System.out.println(anomaly.startTime + " to " + anomaly.endTime + "  : EXPECTED: " + anomaly.expectedVal + " != ACTUAL: " + anomaly.actualVal);
		        		//Store information about the anomaly into the AnomalyDatabase
		        		anomalyDB.AddAnomaly(new PointAnomaly(metric.getSourceNode(), metric.getMetricName(), anomaly.startTime, anomaly.expectedVal, anomaly.actualVal));
		        	}
		        }
		        System.out.println("------------");
	        } //end for
	        
	        //graph the number of anomalies by rough timestamp
	        //TODO untested
	        //graph.update(anomalyDB.GetNumAnomaliesByTimeTimestamps(), anomalyDB.GetNumAnomaliesByTimeValues());
	        
	        //Output all anomalies found while program has been running
	        System.out.println(anomalyDB);
	    } //end while
    } //end main
} //end class
