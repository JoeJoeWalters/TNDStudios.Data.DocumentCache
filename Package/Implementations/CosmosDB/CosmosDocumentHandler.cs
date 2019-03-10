using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Data.Common;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;

namespace TNDStudios.Data.DocumentCache.Cosmos
{
    public class CosmosDocumentHandler<T> : IDocumentHandler<T>
    {
        /// <summary>
        /// Logger inherited from the setup 
        /// </summary>
        private ILogger logger;

        /// <summary>
        /// Public view of the currently connection
        /// </summary>
        public String ConnectionString { get; internal set; }

        /// <summary>
        /// Cosmos client for the document handler
        /// </summary>
        private DocumentClient client;

        /// <summary>
        /// Link to the client collection precached so we don't have to build it each time
        /// </summary>
        private Uri collectionLink;

        /// <summary>
        /// Cosmos Database Name
        /// </summary>
        public String DatabaseName { get; internal set; }

        /// <summary>
        /// Cosmos Collection for this cache
        /// </summary>
        public String DataCollection { get; internal set; }

        /// <summary>
        /// Default Constructor
        /// </summary>
        public CosmosDocumentHandler(ILogger logger, String connectionString, String databaseName, String dataCollection)
        {
            this.ConnectionString = connectionString;
            this.DatabaseName = databaseName;
            this.DataCollection = dataCollection;

            this.logger = logger;
        }

        /// <summary>
        /// Connect to the Cosmos Cache
        /// </summary>
        /// <returns>Success Result</returns>
        private void Connect()
        {
            if (this.client == null)
                this.client = Task.Run(async () => { return await ConnectInternal(); }).Result;
        }

        /// <summary>
        /// Connect to the Cosmos database using the local variables
        /// so it can be used for "reconnecting" if the connection breaks
        /// </summary>
        /// <returns>The newly created document client</returns>
        private async Task<DocumentClient> ConnectInternal()
        {
            // Items derived from the connection string
            String authKey = String.Empty;
            Uri serviceEndPoint = null;

            // The newly created document client that will override the global one
            DocumentClient client = null;

            // Create a new instance of the connection builder to pull out the attributes needed
            DbConnectionStringBuilder builder = new DbConnectionStringBuilder
            {
                ConnectionString = this.ConnectionString
            };

            // Get the Account key from the connection string, leave it as empty for 
            // validation if it cannot be found
            if (builder.TryGetValue("AccountKey", out object key))
                authKey = key.ToString();

            // Get the Uri of the account from the connection string, leave it as "null"
            // if it cannot be found for validation later
            if (builder.TryGetValue("AccountEndpoint", out object uri))
                serviceEndPoint = new Uri(uri.ToString());

            // If we found all the bits needed to connect then connect ..
            if (authKey != String.Empty && serviceEndPoint != null)
                client = new DocumentClient(serviceEndPoint, authKey);

            // Create the required database if it does not already exist
            await client.CreateDatabaseIfNotExistsAsync(
                new Database
                {
                    Id = DatabaseName
                });

            // Create the required collection if it does not already exist
            await client.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri(DatabaseName),
                new DocumentCollection
                {
                    Id = DataCollection
                });

            // Cache the collection link for use later
            this.collectionLink = UriFactory.CreateDocumentCollectionUri(DatabaseName, DataCollection);

            // Log that we have connected 
            logger.LogInformation($"Successfully Connected Document Handler to Cosmos DB - {DatabaseName}/{DataCollection}");

            // Send the new document client back to use
            return client;
        }

        /// <summary>
        /// Send the object to the cache
        /// </summary>
        /// <typeparam name="T">The data type that is being sent to storage</typeparam>
        /// <param name="id">The value of the new document to be put in storage (as the json has to be case sensitive)</param>
        /// <param name="data">The data to be wrapped up in the cache document</param>
        /// <returns>Success Result</returns>
        public Boolean Save(String id, T data)
            => Task.Run(async () => { return await SaveInternal(id, data); }).Result;

        private async Task<Boolean> SaveInternal(
            String id,
            T data)
        {
            Boolean result = false; // Failure by default

            // Wrap the item in a document that can transform the incoming
            // id to the id needed for Cosmos DB to work
            DocumentWrapper<T> document =
                new DocumentWrapper<T>()
                {
                    Id = id,
                    Data = data,
                    CreatedDateTime = DateTime.UtcNow
                };

            // Might be the first time we have tried this, so connect
            Connect();

            try
            {
                // Does the document already exist? If so then it's already there .. 
                await this.client.ReadDocumentAsync(
                    UriFactory.CreateDocumentUri(DatabaseName, DataCollection, document.Id));

                result = true; // Success as it is already there
            }
            catch (DocumentClientException de)
            {
                // Probably couldn't find the document by the id, so we probably need to add it in ..
                if (de.StatusCode == HttpStatusCode.NotFound)
                {
                    // Try and add the document in
                    await this.client.CreateDocumentAsync(collectionLink, document);

                    // Didn't fail so must be good
                    result = true;
                }
                else
                    throw; // Something went really wrong
            }

            return result;
        }
        
        /// <summary>
        /// Get the object from the cache by the id reference
        /// </summary>
        /// <param name="id">The id of the document</param>
        /// <returns>The document from the cache</returns>
        public T Get(string id)
        {
            var response = Task.Run(async () => 
            {
                return await this.client.ReadDocumentAsync(
                    UriFactory.CreateDocumentUri(DatabaseName, DataCollection, id));
            }).Result;

            if (response != null)
                return response.Resource.GetPropertyValue<T>("Data");
            else
                return default(T);
        }

        /// <summary>
        /// Get all items that are marked a having not been processed yet
        /// </summary>
        /// <returns>A list of unprocessed items</returns>
        public List<T> GetToProcess(Int32 maxRecords)
        {
            // Get a reference to the list of items that have not been processed
            IQueryable<DocumentWrapper<T>> queryDocuments = client
                        .CreateDocumentQuery<DocumentWrapper<T>>(collectionLink)
                        .Where(so => !so.Processed);

            // Run the query and cast it to the needed list
            return queryDocuments.Select(document => document.Data).ToList<T>();
        }

        /// <summary>
        /// Mark a set of documents as processed
        /// </summary>
        /// <param name="documentsToMark">A list of document id's to mark as processed</param>
        /// <returns>The id's of the documents that did get marked</returns>
        /// https://github.com/Azure/azure-cosmos-dotnet-v2/blob/f374cc601f4cf08d11c88f0c3fa7dcefaf7ecfe8/samples/code-samples/DocumentManagement/Program.cs#L211
        public List<String> MarkAsProcessed(List<String> documentsToMark)
        {
            List<String> result = new List<string>() { };

            IQueryable<DocumentWrapper<T>> queryDocuments = client
                        .CreateDocumentQuery<DocumentWrapper<T>>(collectionLink)
                        .Where(so => documentsToMark.Contains(so.Id));

            queryDocuments
                .ToList<DocumentWrapper<T>>()
                .ForEach(async document => 
                    {
                        try
                        {
                            // Mark as processed
                            document.Processed = true;
                            document.ProcessedDateTime = DateTime.UtcNow;

                            // Wait for Cosmos to do the update
                            await client.UpsertDocumentAsync(collectionLink, document);

                            // Didn't fail, add it to the success list to be returned
                            result.Add(document.Id);
                        }
                        catch { }
                    });

            return result;
        }

        /// <summary>
        /// Purge (Delete) all items marked as processed
        /// </summary>
        /// <returns>If the purge was successful</returns>
        public Boolean Purge()
        {
            Boolean result = true;

            // Get a reference to the list of items that have been processed
            IQueryable<DocumentWrapper<T>> queryDocuments = client
                        .CreateDocumentQuery<DocumentWrapper<T>>(collectionLink)
                        .Where(so => !so.Processed);

            // Run the query and cast it to the needed list
            queryDocuments
                .Select(document => document.Id)
                .ToList<String>()
                .ForEach(async documentId => 
                    {
                        try
                        {
                            // Async can't return so just skip instead if one has already failed
                            // TODO: Do this a better way later when you have more time
                            if (result)
                                await client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(DatabaseName, DataCollection, documentId));
                        }
                        catch
                        {
                            result = false;
                        }
                    });

            return result; // Got to the end
        }
    }
}
