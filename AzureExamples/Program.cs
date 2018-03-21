using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Configuration;
using Microsoft.Azure;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorageReference
{
    class Program
    {
        static void Main(string[] args)
        {
            // Module 1: learn about Azure Storage Blob Examples
            // AzureStorageBlobExamples();

            // Module 2: Learn about Azure Storage Table Examples
            AzureStorageTableExamples();
        }


        static void AzureStorageBlobExamples()
        {
            // Getting configuration from the App.config file and get access to the CloudStorageAccount.
            // Azure access Key should is stored in the App.config
            String storageKeyValue = CloudConfigurationManager.GetSetting("AzureStorageAccount");

            // Just in case to check whether reading the correct value
            Console.WriteLine("Storage Account Key Used" + storageKeyValue);

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageKeyValue);

            // Getting the Azure Client 
            CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

            // Getting reference for the container
            CloudBlobContainer cloudContainer = cloudBlobClient.GetContainerReference("images");

            // Create the container (folder) if does not exists
            cloudContainer.CreateIfNotExists();

            // Get the reference for the Blob in Azure
            CloudBlockBlob blobCloud = cloudContainer.GetBlockBlobReference("photo.png");


            // Learning 1: Copy a file from local path in to Azure Blob storage

            //Open a local file and upload to the Azure
            using (var fileStream = System.IO.File.OpenRead(@"C:\Users\Administrator\Pictures\photo.jpg"))
            {
                blobCloud.UploadFromStream(fileStream);
                Console.WriteLine("File is uploaded to the cloud");
            }

            // Learning 2: Download a blob from Azure to the local host

            // Just copy a blob from cloud to a local file. Assume the same file which has been uploaded in the previous step
            blobCloud.DownloadToFile(@"C:\Users\Administrator\Pictures\photocopy.jpg", FileMode.CreateNew);

            // Learning 3: List all blob objects URI from a container

            //List all blobs in a container 
            var blobs = cloudContainer.ListBlobs();
            foreach (var blob in blobs)
            {
                Console.Write(blob.Uri);
            }

            // Learning 4: List properties of a continer (Folder) and listing metadata of a container

            cloudContainer.FetchAttributes();
            Console.WriteLine(cloudContainer.Properties.LastModified);
            Console.WriteLine(cloudContainer.Properties.ETag);
            var metaData = cloudContainer.Metadata;
            foreach (var metaDataItem in metaData)
            {
                Console.Write("Key " + metaDataItem.Key + " & ");
                Console.WriteLine("Value" + metaDataItem.Value);
            }

            // Learning 5: Setting metaData for a container

            // Method 1
            cloudContainer.Metadata.Add("SampleKey", "SampleValue");
            // Method 2
            cloudContainer.Metadata["SecondSample"] = "Second Value";

            // Dont ask me why FetchAttributes() and SetMetadata() ! why not SetAttributes() or GetMetaData? Microsoft Way!
            cloudContainer.SetMetadata();


            // Learning 6: Setting permission for a container
            BlobContainerPermissions permissions = cloudContainer.GetPermissions();
            Console.WriteLine("Container permission " + permissions.PublicAccess.ToString());
            foreach (var sharedAccessPolicy in permissions.SharedAccessPolicies)
            {
                Console.WriteLine(sharedAccessPolicy.Key.ToString() + " = " + sharedAccessPolicy.Value.ToString());
            }
            // In order to as per the parent container
            permissions.PublicAccess = BlobContainerPublicAccessType.Container;
            // In order to remove the public access
            permissions.PublicAccess = BlobContainerPublicAccessType.Off;
            //Finally set the permission
            cloudContainer.SetPermissions(permissions);



            // Learning 7: Azure copy from one blob to another

            // Create a new Block Blog Reference
            CloudBlockBlob copyBlob = cloudContainer.GetBlockBlobReference("photo-copy.jpg");
            // Copy the original - blobCloud to the copyBlob
            copyBlob.StartCopy(new Uri(blobCloud.Uri.AbsoluteUri));


            // Learning 8: Copy all blobs from one container to another
            CloudBlobContainer sourceContainer = cloudContainer;
            CloudBlobContainer targetContainer = cloudBlobClient.GetContainerReference("newimages");
            targetContainer.CreateIfNotExists();
            foreach (var blob in sourceContainer.ListBlobs())
            {
                var sourceBlob = blob as CloudBlob;
                Console.WriteLine("Source Blob " + sourceBlob.Name);
                CloudBlockBlob newBlob = targetContainer.GetBlockBlobReference(sourceBlob.Name);
                newBlob.StartCopy(new Uri(blob.Uri.AbsoluteUri));
            }


            // Learning 9: Rename the blob
            CloudBlockBlob sourceBlockBlob = cloudContainer.GetBlockBlobReference("photo-copy.jpg");
            CloudBlockBlob targetBlockBlob = cloudContainer.GetBlockBlobReference("copy-photo.jpg");
            targetBlockBlob.StartCopy(new Uri(sourceBlockBlob.Uri.AbsoluteUri));
            while (targetBlockBlob.CopyState.Status == CopyStatus.Pending)
            {
                // Sleep for 3 seconds
                System.Threading.Thread.Sleep(2000);
            }
            sourceBlockBlob.Delete();


            // Learning 10: Appending to a blob
            DateTime date = DateTime.Today;
            CloudBlobContainer logContainer = cloudBlobClient.GetContainerReference("logs");
            CloudAppendBlob logBlog = logContainer.GetAppendBlobReference(string.Format("{0}{1}", date.ToString("yyyyMMdd"), ".log"));
            logContainer.CreateIfNotExists();

            // If the append blog does not exists, create one
            if (!logBlog.Exists())
            {
                logBlog.CreateOrReplace();
            }

            // AppendText
            logBlog.AppendText(string.Format("{0} : Azure is rocking in the cloud space at ", date.ToString("HH:MM:ss")));
            // Similar to the AppendText, there are
            // logBlog.AppendBlock
            // logBlog.AppendFromByteArray
            // logBlog.AppendFromFile
            // logBlog.AppendFromStream

            // Finally display the content of the log file.
            Console.WriteLine(logBlog.DownloadText());

            // Learning 11: Multiple Chunk of file upload to Azure
            AzureStorageReference.Program.uploadLargeFiles(cloudBlobClient, @"C:\Users\Administrator\Pictures");


            //Learning 12: Upload using Async

            AsyncCallback callBack = new AsyncCallback(x => Console.WriteLine("Copy Async Completed"));
            CloudBlockBlob copyAsync = cloudContainer.GetBlockBlobReference("newphoto.png");

            copyAsync.BeginStartCopy(blobCloud, callBack, null);

            Console.WriteLine("Press any key to continue");
            Console.ReadKey();
        }

        private async static Task uploadLargeFiles(CloudBlobClient cloudBlobClient, String localDirectory)
        {
            int max_outstanding = 100;

            CloudBlobContainer[] containers = await GetRandomContainersAsync(cloudBlobClient);

            // Define the BlobRequestionOptions on the upload.
            // This includes defining an exponential retry policy to ensure that failed connections are retried with a backoff policy. As multiple large files are being uploaded
            // large block sizes this can cause an issue if an exponential retry policy is not defined.  Additionally parallel operations are enabled with a thread count of 8
            // This could be should be multiple of the number of cores that the machine has. Lastly MD5 hash validation is disabled for this example, this improves the upload speed.
            BlobRequestOptions options = new BlobRequestOptions
            {
                ParallelOperationThreadCount = 8,
                DisableContentMD5Validation = true,
                StoreBlobContentMD5 = false
            };

            // Counter for files
            int count = 0;
            int completed_count = 0;
            Stopwatch time = Stopwatch.StartNew();

            Console.WriteLine("Iterating in directory: {0}", localDirectory);

            // Create a new instance of the SemaphoreSlim class to define the number of threads to use in the application.
            SemaphoreSlim sem = new SemaphoreSlim(max_outstanding, max_outstanding);

            List<Task> tasks = new List<Task>();

            //CloudBlobContainer largeContainer = cloudBlobClient.GetContainerReference("large-files");
            var directoryListing = Directory.GetFiles(localDirectory);
            foreach (var file in directoryListing)
            {
                // Get the filename from the path given
                String fileName = Path.GetFileName(file);
                Console.WriteLine("Processing file " + fileName);
                var container = containers[count % 3];
                CloudBlockBlob largeBlockBlob = container.GetBlockBlobReference(fileName);
                largeBlockBlob.StreamWriteSizeInBytes = 100 * 1024 * 1024;

                await sem.WaitAsync();

                tasks.Add(largeBlockBlob.UploadFromFileAsync(file, null, options, null).ContinueWith((t) =>
                {
                    sem.Release();
                    Interlocked.Increment(ref completed_count);
                }));
                count++;
            }
            await Task.WhenAll(tasks);
            time.Stop();
            Console.WriteLine("Upload has been completed in {0} seconds. Press any key to continue", time.Elapsed.TotalSeconds.ToString());
        }

        // This Asynchronous task is used to create random containers with the storage account.
        // A collection of CloudBlobContainers is returned from this helper task to the caller.
        public static async Task<CloudBlobContainer[]> GetRandomContainersAsync(CloudBlobClient cloudBlobClient)
        {
            CloudBlobClient blobClient = cloudBlobClient;
            CloudBlobContainer[] blobContainers = new CloudBlobContainer[5];
            for (int i = 0; i < blobContainers.Length; i++)
            {
                blobContainers[i] = blobClient.GetContainerReference(System.Guid.NewGuid().ToString());
                try
                {
                    await blobContainers[i].CreateIfNotExistsAsync();
                    Console.WriteLine("Created container {0}", blobContainers[i].Uri);
                }
                catch (StorageException)
                {
                    Console.WriteLine("If you are using the storage emulator, please make sure you have started it. Press the Windows key and type Azure Storage to select and run it from the list of applications - then restart the sample.");
                    Console.ReadLine();
                    throw;
                }
            }

            return blobContainers;
        }


        public class TenantEntity : TableEntity
        {
            public TenantEntity(string lastName, string firstName, string region)
            {
                this.firstName = firstName;
                this.PartitionKey = region;
                this.RowKey = lastName;
                this.lastName = lastName;
            }

            public TenantEntity() { }

            public string firstName { get; set; }

            public string lastName { get; set; }

            public string Email { get; set; }

            public string PhoneNumber { get; set; }
        }

        static void AzureStorageTableExamples()
        {
            // Getting configuration from the App.config file and get access to the CloudStorageAccount.
            // Azure access Key should is stored in the App.config
            String storageKeyValue = CloudConfigurationManager.GetSetting("AzureStorageAccount");

            // Just in case to check whether reading the correct value
            Console.WriteLine("Storage Account Key Used" + storageKeyValue);

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageKeyValue);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Retrieve a reference to the table.
            CloudTable table = tableClient.GetTableReference("tenant");

            table.CreateIfNotExists();

            // Lesson 1 Table Insert

            TenantEntity tenant1 = new TenantEntity("Neson", "De Jesus", "Canada");
            tenant1.Email = "nelson.dejesus@cibc.com";
            tenant1.PhoneNumber = "911-911-9111";

            TableOperation tOper = TableOperation.Insert(tenant1);

            // Lesson 2 Batch Insert
            TableBatchOperation batchInsertOperation = new TableBatchOperation();

            TenantEntity tenant2 = new TenantEntity("Vivek", "Menon", "USA");
            tenant2.Email = "vivek.menon@cibc.com";
            tenant2.PhoneNumber = "911-911-9111";

            batchInsertOperation.Insert(tenant2);

            TenantEntity tenant3 = new TenantEntity("Sanders", "John", "USA");
            tenant3.Email = "john.sanders@hnahonda.com";
            tenant3.PhoneNumber = "911-911-9111";

            batchInsertOperation.Insert(tenant3);

            table.ExecuteBatch(batchInsertOperation);

            // Lession 3: Accesing the table data
            TableQuery<TenantEntity> query = new TableQuery<TenantEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", 
                QueryComparisons.Equal, "USA"));
            foreach(TenantEntity tenant in table.ExecuteQuery(query))
            {
                Console.WriteLine("{0}, {1}, {2}, {3}", tenant.firstName, tenant.lastName, tenant.Email, tenant.PhoneNumber);
            }


            // Lesson 4: Multiple condition in where clause
            TableQuery<TenantEntity> queryMultipleWhereContidion = new TableQuery<TenantEntity>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "USA"),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("lastName", QueryComparisons.Equal, "Sanders")));

            foreach(TenantEntity tenant in table.ExecuteQuery(queryMultipleWhereContidion))
            {
                Console.WriteLine("{0}, {1}, {2}, {3}", tenant.firstName, tenant.lastName, tenant.Email, tenant.PhoneNumber);
            }


            // Lesson 5: Single record retrieval
            TableOperation retrieveOperation = TableOperation.Retrieve<TenantEntity>("Sanders", "John");
            TableResult retrievedResult = table.Execute(retrieveOperation);
            if (retrievedResult.Result != null)
            {
                Console.WriteLine(((TenantEntity)retrievedResult.Result).PhoneNumber);
            }
            else
            {
                Console.WriteLine("The phone number could not be retrieved.");
            }
            Console.ReadLine();
        }
    }
}
