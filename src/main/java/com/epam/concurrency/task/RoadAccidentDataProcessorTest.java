package com.epam.concurrency.task;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

public class RoadAccidentDataProcessorTest {
	
	@RunWith(MockitoJUnitRunner.class)
	public class AccidentDataProcessorTest {
		
		private AccidentDataProcessor dataProcessor;

		@Mock
	    private AccidentDataReader accidentDataReader;
		
		@Mock
	    private AccidentDataEnricher accidentDataEnricher;
		
		@Mock
	    private AccidentDataWriter accidentDataWriter;
	    
		List<String> fileQueue;
		
		@SuppressWarnings("unchecked")
		@Before 
		public void init(){
			fileQueue = mock(ArrayList.class);
		}
		
		@Test
		public void testInitReturnVoid(){
			dataProcessor = new AccidentDataProcessor(accidentDataReader, accidentDataEnricher, accidentDataWriter);
			dataProcessor.setFileQueue(fileQueue);
			dataProcessor.init();
			verify(fileQueue).add("src/main/resources/DfTRoadSafety_Accidents_2011.csv");
			verify(fileQueue).add(anyString());
			verify(accidentDataWriter).init("target/DfTRoadSafety_Accidents_consolidated.csv");
		}
		
		@SuppressWarnings("unchecked")
		@Test
		public void testParallellyProcessFileReturnVoid(){
			dataProcessor = new AccidentDataProcessor();
			
			dataProcessor.setFileQueue(fileQueue);
			dataProcessor.init();
			
			Iterator<String> fileQueueIterator;
			fileQueueIterator = mock(Iterator.class);
		    when(fileQueueIterator.hasNext()).thenReturn(true,false);
		    when(fileQueueIterator.next()).thenReturn("src/main/resources/DfTRoadSafety_Accidents_2011.csv");
		    when(fileQueue.iterator()).thenReturn(fileQueueIterator);
			dataProcessor.process("parallel");
		}
		
		@Test
		public void testFileQueueIsNotEmpty(){
			dataProcessor = new AccidentDataProcessor();
			dataProcessor.init();
			assertNotNull(dataProcessor.getFileQueue());
		}
		
	}

}
