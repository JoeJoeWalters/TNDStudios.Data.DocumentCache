using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Data.Common;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

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
        public Boolean SendToCache(String id, T data)
            => Task.Run(async () => { return await SendToCacheInternal(id, data); }).Result;

        private async Task<Boolean> SendToCacheInternal(
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
                    await this.client.CreateDocumentAsync(
                        UriFactory.CreateDocumentCollectionUri(DatabaseName, DataCollection), document);

                    // Didn't fail so must be good
                    result = true;
                }
                else
                    throw; // Something went really wrong
            }

            return result;
        }
    }
}
