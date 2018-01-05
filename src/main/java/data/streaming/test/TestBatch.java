package data.streaming.test;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import data.streaming.utils.MongoUtils;

public class TestBatch {
	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

	public void updateRatings() {
		final Runnable ratings = new Runnable() {
			public void run() {
				System.out.println("UPDATING RATINGS ("+new Date()+")");
				MongoUtils.addRatings();
			}
		};
		scheduler.scheduleAtFixedRate(ratings, 0, 1, TimeUnit.HOURS);
		
	}

	public static void main(String[] args) {
		MongoUtils.initialize();
		TestBatch batch= new TestBatch();
		batch.updateRatings();
	}
}