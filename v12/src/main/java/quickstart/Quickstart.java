package quickstart;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.Arrays;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;

public class Quickstart {
    static File createTempFile() throws IOException {

        // Here we are creating a temporary file to use for download and upload to Blob
        // storage
        File sampleFile = null;
        sampleFile = File.createTempFile("sampleFile", ".txt");
        System.out.println(">> Creating a sample file at: " + sampleFile.toString());
        Writer output = new BufferedWriter(new FileWriter(sampleFile));
        output.write("Hello Azure Storage blob quickstart.");
        output.close();

        return sampleFile;
    }

    public static void main(String[] args) throws IOException
    {

        // Creating a sample file to use in the sample
        File source = null;
        String downloadedFilePath = "downloadedFile.txt";
        String blobName = "myblob";
        if(args.length>0)
        {
            source=new File(args[0]);
            downloadedFilePath="downloaded."+source.getName();
            blobName=source.getName();
        }
        else
        {
            source = createTempFile();
        }
        String containerName = "mycontainer";
        if(args.length>2)
        {
            containerName=args[1];


        }

        // Retrieve the credentials and initialize SharedKeyCredentials
        String accountName = System.getenv("AZURE_STORAGE_ACCOUNT");
        String accountKey = System.getenv("AZURE_STORAGE_ACCESS_KEY");
        String endpoint = "https://" + accountName + ".blob.core.windows.net";

        // Create a SharedKeyCredential
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);

        // Create a blobServiceClient
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
            .endpoint(endpoint)
            .credential(credential)
            .buildClient();

        // Create a containerClient
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);

        // Create a container
        if(!blobContainerClient.exists())
        {
            blobServiceClient.createBlobContainer(containerName);
            System.out.printf("Creating a container : %s %n", blobContainerClient.getBlobContainerUrl());
        }
        else
        {
            System.out.printf("Container already exists : %s %n", blobContainerClient.getBlobContainerUrl());

        }

        // Create a BlobClient to run operations on Blobs
        BlobClient blobClient = blobContainerClient.getBlobClient(blobName);

        // Listening for commands from the console
        System.out.println("Enter a command");
        System.out.println("(U)Upload Blob | (L)List Blobs | (G)Get Blob | (D)Delete Blobs | (E)Exit");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {

            System.out.println("# Enter a command : ");
            String input = reader.readLine();

            switch (input) {

            // Upload a blob from a File
            case "U":
                System.out.println("Uploading the sample file into the container from a file: "
                        + blobContainerClient.getBlobContainerUrl());
            {
                long start = System.currentTimeMillis();
                uploadFilePath(source, blobContainerClient);
                System.out.println("Time spent uploading: "+source+ ": "+(System.currentTimeMillis()-start));
            }

                break;

            // List Blobs
            case "L":
                System.out.println("Listing blobs in the container: " + blobContainerClient.getBlobContainerUrl());
                blobContainerClient.listBlobs()
                        .forEach(
                            blobItem -> System.out.println("This is the blob name: " + blobItem.getName()));
                break;

            // Download a blob to local path
            case "G":
                System.out.println("Get(Download) the blob: " + blobClient.getBlobUrl());
                String name=blobContainerClient.getBlobContainerName();
                File download = new File("download", name);
                download.mkdirs();
                long start=System.currentTimeMillis();
                blobContainerClient.listBlobs()
                        .forEach(
                                blobItem ->{
                                    System.out.println("This is the blob name: " + blobItem.getName());
                                    BlobClient theBlobClient = blobContainerClient.getBlobClient(blobItem.getName());
                                    String downloadPath = new File(download, blobItem.getName()).getAbsolutePath();
                                    theBlobClient.downloadToFile(downloadPath);
                                    System.out.println("Downloaded: "+downloadPath);

                                }

                                );
                System.out.println("Downloaded to: "+download+" "+(System.currentTimeMillis()-start) );
                break;

            // Delete a blob
            case "D":
                System.out.println("Delete the blob: " + blobClient.getBlobUrl());
                blobClient.delete();
                System.out.println();
                break;

            // Exit
            case "E":
                File downloadFile = new File(downloadedFilePath);
                System.out.println("Cleaning up the sample and exiting.");

                downloadFile.delete();
                System.exit(0);
                break;

            default:
                break;
            }
        }
    }

    private static void uploadFilePath(File source, BlobContainerClient blobContainerClient)
    {
        if(source.isFile())
        {
            System.out.println("Uploading: "+source);
            BlobClient blobC = blobContainerClient.getBlobClient(source.getName());
            blobC.uploadFromFile(source.toPath().toString());
        }
        else
        {
            File[] files = source.listFiles();
            Arrays.asList(files).forEach(file->uploadFilePath(file, blobContainerClient));
        }
    }
}
