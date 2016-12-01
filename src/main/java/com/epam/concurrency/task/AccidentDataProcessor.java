package com.epam.concurrency.task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epam.data.RoadAccident;

/**
 * Created by Tanmoy on 6/17/2016.
 */
public class AccidentDataProcessor {

    private static final String FILE_PATH_1 = "src/main/resources/DfTRoadSafety_Accidents_2010.csv";
    private static final String FILE_PATH_2 = "src/main/resources/DfTRoadSafety_Accidents_2011.csv";
    private static final String FILE_PATH_3 = "src/main/resources/DfTRoadSafety_Accidents_2012.csv";
    private static final String FILE_PATH_4 = "src/main/resources/DfTRoadSafety_Accidents_2013.csv";

    private static final String OUTPUT_FILE_PATH = "target/DfTRoadSafety_Accidents_consolidated.csv";

    private static final int DATA_PROCESSING_BATCH_SIZE = 10000;

    private AccidentDataReader accidentDataReader = new AccidentDataReader();
    private AccidentDataEnricher accidentDataEnricher = new AccidentDataEnricher();
    private AccidentDataWriter accidentDataWriter = new AccidentDataWriter();

    private List<String> fileQueue = new ArrayList<String>();

    private Logger log = LoggerFactory.getLogger(AccidentDataProcessor.class);


    public void init(){
        fileQueue.add(FILE_PATH_1);
        //fileQueue.add(FILE_PATH_2);
        //fileQueue.add(FILE_PATH_3);
        //fileQueue.add(FILE_PATH_4);

        accidentDataWriter.init(OUTPUT_FILE_PATH);
    }

    public void process(String processMethod){
    	
        for (String accidentDataFile : fileQueue){
            log.info("Starting to process {} file ", accidentDataFile);
            accidentDataReader.init(DATA_PROCESSING_BATCH_SIZE, accidentDataFile);
            if("parallel".equalsIgnoreCase(processMethod)){
            	parallelProcessFile();
            }else{
            	processFile();
            }
        }
    }

    private void processFile(){
        int batchCount = 1;
        while (!accidentDataReader.hasFinished()){
            List<RoadAccident> roadAccidents = accidentDataReader.getNextBatch();
            log.info("Read [{}] records in batch [{}]", roadAccidents.size(), batchCount++);
            List<RoadAccidentDetails> roadAccidentDetailsList = accidentDataEnricher.enrichRoadAccidentData(roadAccidents);
            log.info("Enriched records");
            accidentDataWriter.writeAccidentData(roadAccidentDetailsList);
            log.info("Written records");
        }
    }
    
	private void parallelProcessFile() {
		int batchCount = 1;
		int threadNum = 5;
		ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
		while (!accidentDataReader.hasFinished()) {
			List<RoadAccident> roadAccidents = accidentDataReader.getNextBatch();
			if (roadAccidents != null && roadAccidents.size() > 0) {
				log.info("Read [{}] records in batch [{}]", roadAccidents.size(), batchCount++);
				executorService.execute(new ParallelDataProcessor(roadAccidents, accidentDataEnricher, accidentDataWriter));
			}
		}
		Util.sleepToSimulateDataHeavyProcessing(1000);
		if (executorService.isTerminated()) {
            	log.info("Success");
		}
		executorService.shutdown();

	}

    public static void main(String[] args) {
        AccidentDataProcessor dataProcessor = new AccidentDataProcessor();
        long start = System.currentTimeMillis();
        dataProcessor.init();
        dataProcessor.process("parallel");
        //dataProcessor.process("");
        long end = System.currentTimeMillis();
        System.out.println("Process finished in s : " + (end-start)/1000);
    }

}

class ParallelDataProcessor implements Runnable {
	private Logger log = LoggerFactory.getLogger(ParallelDataProcessor.class);

	private AccidentDataEnricher accidentDataEnricher;

	private List<RoadAccident> roadAccidents;

	private AccidentDataWriter accidentDataWriter;

	public ParallelDataProcessor( List<RoadAccident> roadAccidents, AccidentDataEnricher accidentDataEnricher,
			AccidentDataWriter accidentDataWriter) {
		this.accidentDataEnricher = accidentDataEnricher;
		this.roadAccidents = roadAccidents;
		this.accidentDataWriter = accidentDataWriter;
	}

	@Override
	public void run() {
		List<RoadAccidentDetails> roadAccidentDetailsList = accidentDataEnricher.enrichRoadAccidentData(roadAccidents);
		log.info(String.format("Enriched records"));
		accidentDataWriter.writeAccidentData(roadAccidentDetailsList);
		log.info(String.format("Written records"));

	}
}