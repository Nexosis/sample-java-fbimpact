package com.nexosis.samples;

import com.nexosis.impl.NexosisClient;
import com.nexosis.impl.NexosisClientException;
import com.nexosis.model.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.*;
import java.util.*;

public class ImpactFacebookLikes {

    private static final String path = System.getProperty("user.dir") + "/data";

    public static void main(String[] args) throws InterruptedException, IOException {
        NexosisClient client = new NexosisClient();

        try {
            String dataSetName = "facebook-data-v2";
            String eventName = "api-announcement-reloaded";
            String sourceFile = path + "/facebook-data.csv";
            String resultsFile = path + "/impact-results.csv";

            // Load and submit data to Nexosis API
            loadDataSet(client, dataSetName, sourceFile);
            // Start Impact Run
            UUID sessionID = runImpactAnalysis(client, dataSetName, eventName);

            // Wait for Session Completion
            waitForSessionCompletion(client, sessionID);
            // Retrieve Session Results and save impact output to CSV file.
            SessionResult result = getImpactResults(client, sessionID, resultsFile);

            // Print out the Session Metrics.
            for (Map.Entry<String, Double> entry : result.getMetrics().getAdditionalProperties().entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }
        } catch (NexosisClientException nce) {
            System.out.println("Status: " + nce.getStatusCode());
            System.out.println("Status: " + nce.getMessage());

            if ((nce.getErrorResponse()) != null && nce.getErrorResponse().getErrorDetails() != null) {
                for (Map.Entry<String, Object> entry : nce.getErrorResponse().getErrorDetails().entrySet()) {
                    System.out.println(entry.getKey() + ": " + entry.getValue());
                }
            }

            System.out.println("Error Response: " + nce.getErrorResponse());
        }
    }

    private static void loadDataSet(NexosisClient client, String dataSetName, String fileName) throws IOException, NexosisClientException {
        // Load the CSV into a DataSet object
        DataSetData loadedDataSet = loadDataSetFile(fileName);
        // Create the dataset on the server
        client.getDataSets().create(dataSetName, loadedDataSet);

        DataSetList dataSets = client.getDataSets().list();
        System.out.println("Number of datasets: " + dataSets.getItems().size());
        for (DataSetSummary data : dataSets.getItems()) {
            System.out.println("Name: " + data.getDataSetName());
        }
    }

    private static DataSetData loadDataSetFile(String fileName) throws IOException {
        File file = new File(fileName);
        LineIterator it = FileUtils.lineIterator(file, "UTF-8");
        DataSetData data = new DataSetData();

        try {

            List<Map<String, String>> rows = new ArrayList<>();
            String[] headers = null;

            while (it.hasNext()) {
                Map<String, String> row = new HashMap<>();
                String line = it.nextLine();
                // split csv row into a string array
                String[] cells = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

                if (headers == null) {
                    headers = new String[cells.length];
                    System.arraycopy(cells, 0, headers, 0, cells.length);

                    // Setup Default Columns MetaData
                    Columns metadata = new Columns();
                    metadata.setColumnMetadata("date", DataType.DATE, DataRole.TIMESTAMP);
                    metadata.setColumnMetadata("page_views", DataType.NUMERIC, DataRole.NONE);
                    metadata.setColumnMetadata("daily_paid_likes", DataType.NUMERIC, DataRole.NONE);
                    metadata.setColumnMetadata("paid_post_impressions", DataType.NUMERIC, DataRole.NONE);
                    metadata.setColumnMetadata("organic_post_impressions", DataType.NUMERIC, DataRole.NONE);
                    metadata.setColumnMetadata("total_page_likes", DataType.NUMERIC, DataRole.NONE);
                    metadata.setColumnMetadata("daily_organic_likes", DataType.NUMERIC, DataRole.NONE);
                    metadata.setColumnMetadata("amount_spent", DataType.NUMERIC, DataRole.NONE);
                    data.setColumns(metadata);
                } else {
                    for (int i = 0; i < headers.length; i++) {
                        // Hack to remove dollar symbol from data.
                        if (i == 0) {
                            DateTimeFormatter formatter = DateTimeFormat.forPattern("MM/dd/yyyy");
                            cells[i] = formatter.parseDateTime(cells[i]).toString();
                        }
                        cells[i] = cells[i].replaceAll("\\$","");
                        row.put(headers[i].toLowerCase().replace(' ', '_'), cells[i]);
                    }
                    rows.add(row);
                }
            }
            data.setData(rows);
        } finally {
            it.close();
        }
        return data;
    }

    private static UUID runImpactAnalysis(NexosisClient client, String dataSetName, String eventName) throws NexosisClientException {
        // Change metadata for Impact analysis
        Columns columns = new Columns();
        columns.setColumnMetadata("date", DataType.DATE, DataRole.TIMESTAMP);
        columns.setColumnMetadata("page_views", DataType.NUMERIC, DataRole.FEATURE);
        columns.setColumnMetadata("daily_paid_likes", DataType.NUMERIC, DataRole.NONE);
        columns.setColumnMetadata("paid_post_impressions", DataType.NUMERIC, DataRole.NONE);
        columns.setColumnMetadata("organic_post_impressions", DataType.NUMERIC, DataRole.NONE);
        columns.setColumnMetadata("total_page_likes", DataType.NUMERIC, DataRole.TARGET);
        columns.setColumnMetadata("daily_organic_likes", DataType.NUMERIC, DataRole.NONE);
        columns.setColumnMetadata("amount_spent", DataType.NUMERIC, DataRole.FEATURE);

        SessionResponse response = client.getSessions().analyzeImpact(
                dataSetName,
                columns,
                eventName,
                DateTime.parse("2017-03-15T00:00:00Z"),
                DateTime.parse("2017-06-28T00:00:00Z"),
                ResultInterval.DAY
        );

        return response.getSessionId();
    }

    private static SessionResult getImpactResults(NexosisClient client, UUID sessionID, String resultsFile) throws IOException, NexosisClientException {
        // Write output to file.
        File writeFile = new File(resultsFile);
        writeFile.createNewFile();

        OutputStream outputStream = new FileOutputStream(resultsFile);
        ReturnsStatus analysisResult = client.getSessions().getResults(sessionID, outputStream);
        System.out.println("Results written to " + resultsFile);
        System.out.println(analysisResult.getSessionStatus());
        outputStream.flush();
        outputStream.close();
        // Retrieve Session Results
        SessionResult result = client.getSessions().getResults(sessionID);
        return result;
    }

    private static void waitForSessionCompletion(NexosisClient client, UUID sessionID) throws NexosisClientException, InterruptedException {
        boolean keepWaiting = true;
        System.out.print("Waiting for job to complete");

        while (keepWaiting) {
            System.out.print(".");
            SessionResultStatus results = client.getSessions().getStatus(sessionID);

            switch(results.getStatus()) {
                case COMPLETED:
                case CANCELLED:
                    keepWaiting = false;
                    break;
                default:
                    keepWaiting = true;
                    break;
            }

            if (!keepWaiting)
                break;

            Thread.sleep(5000);
        }
        System.out.println();
        System.out.println("Done.");
    }
}