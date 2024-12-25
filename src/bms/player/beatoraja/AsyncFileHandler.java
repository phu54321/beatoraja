package bms.player.beatoraja;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

public class AsyncFileHandler extends Handler {
    private final BlockingQueue<LogRecord> logQueue;
    private final FileHandler fileHandler;
    private volatile boolean running;
    private final Thread workerThread;

    public AsyncFileHandler(String pattern) throws IOException {
        this.fileHandler = new FileHandler(pattern);
        this.logQueue = new LinkedBlockingQueue<>();
        this.running = true;

        // Start the background worker thread
        this.workerThread = new Thread(() -> {
            while (running) {
                try {
                    while (true) {
                        LogRecord record = logQueue.poll();
                        if (record != null) {
                            fileHandler.publish(record);
                        } else break;
                    }
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        this.workerThread.setDaemon(true);
        this.workerThread.start();
    }

    @Override
    public void publish(LogRecord record) {
        if (isLoggable(record)) {
            try {
                logQueue.put(record); // Add the log record to the queue
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("AsyncFileHandler interrupted while queuing log record.");
            }
        }
    }

    @Override
    public void flush() {
        // Wait for the queue to be processed
        while (!logQueue.isEmpty()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        fileHandler.flush();
    }

    @Override
    public void close() throws SecurityException {
        running = false;
        try {
            workerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        fileHandler.close();
    }

    @Override
    public void setFormatter(Formatter newFormatter) throws SecurityException {
        super.setFormatter(newFormatter);
        fileHandler.setFormatter(newFormatter);
    }

    @Override
    public void setLevel(Level newLevel) throws SecurityException {
        super.setLevel(newLevel);
        fileHandler.setLevel(newLevel);
    }
}