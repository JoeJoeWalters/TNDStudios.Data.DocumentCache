using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace TNDStudios.Data.DocumentCache.Implementations.Memory
{
    public class MemoryDocumentHandler<T> : IDocumentHandler<T>
    {
        /// <summary>
        /// Logger inherited from the setup 
        /// </summary>
        private ILogger logger;

        /// <summary>
        /// Where documents are stored in memory
        /// </summary>
        private Dictionary<String, DocumentWrapper<T>> documentCache;

        /// <summary>
        /// Public view of the currently connection
        /// </summary>
        public String ConnectionString { get; internal set; } = String.Empty;

        /// <summary>
        /// Cosmos Database Name
        /// </summary>
        public String DatabaseName { get; internal set; } = String.Empty;

        /// <summary>
        /// Cosmos Collection for this cache
        /// </summary>
        public String DataCollection { get; internal set; } = String.Empty;

        /// <summary>
        /// Default Constructor
        /// </summary>
        public MemoryDocumentHandler(ILogger logger, String connectionString, String databaseName, String dataCollection)
        {
            this.ConnectionString = connectionString;
            this.DatabaseName = databaseName;
            this.DataCollection = dataCollection;

            this.logger = logger;

            this.documentCache = new Dictionary<String, DocumentWrapper<T>>();
        }

        /// <summary>
        /// Send the object to the cache
        /// </summary>
        /// <typeparam name="T">The data type that is being sent to storage</typeparam>
        /// <param name="id">The value of the new document to be put in storage (as the json has to be case sensitive)</param>
        /// <param name="data">The data to be wrapped up in the cache document</param>
        /// <returns>Success Result</returns>
        public bool Save(string id, T data)
        {
            // Add the document to the dictionary of documents
            try
            {
                documentCache[id] = new DocumentWrapper<T>()
                {
                    Id = id,
                    Data = data,
                    CreatedDateTime = DateTime.UtcNow
                };
            }
            catch { }

            // Did it get added?
            return documentCache.ContainsKey(id);
        }

        /// <summary>
        /// Get the object from the cache by the id reference
        /// </summary>
        /// <param name="id">The id of the document</param>
        /// <returns>The document from the cache</returns>
        public T Get(string id)
        {
            if (documentCache.ContainsKey(id))
                return documentCache[id].Data;
            else
                return default(T);
        }

        /// <summary>
        /// Get all items that are marked a having not been processed yet
        /// </summary>
        /// <returns>A list of unprocessed items</returns>
        public List<T> GetToProcess(Int32 maxRecords) =>
            documentCache
                .Where(keyValuePair => !keyValuePair.Value.Processed)
                .Select(keyValuePair => keyValuePair.Value.Data)
                .ToList<T>();

        /// <summary>
        /// Mark a set of documents as processed
        /// </summary>
        /// <param name="documentsToMark">A list of document id's to mark as processed</param>
        /// <returns>The id's of the documents that did get marked</returns>
        public List<String> MarkAsProcessed(List<String> documentsToMark)
        {
            try
            {
                // Attempt to mark the items
                documentCache
                .Where(document => documentsToMark.Contains(document.Key))
                .ToList()
                .ForEach(document =>
                    {
                        document.Value.Processed = true;
                        document.Value.ProcessedDateTime = DateTime.UtcNow;
                    });
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failure to purge processed items from document cache");
            }

            // Return a list of the items that successfully marked as processed
            try
            {
                return documentCache
                    .Where(document =>
                        (documentsToMark.Contains(document.Key) && document.Value.Processed))
                    .Select(document => document.Key)
                    .ToList<String>();
            }
            catch(Exception ex)
            {
                logger.LogError(ex, "Failure to return keys of items marked as processed from document cache");
            }

            return new List<string>(); // Catastrophic failure where couldn't stamp and couldn't return
        }

        /// <summary>
        /// Purge (Delete) all items marked as processed
        /// </summary>
        /// <returns>If the purge was successful</returns>
        public Boolean Purge()
        {
            try
            {
                // Because of parallelism
                List<String> keysToPurge = documentCache
                    .Where(document => document.Value.Processed)
                    .Select(document => document.Key)
                    .ToList<String>();

                // Remove the keys
                keysToPurge.ForEach(key => documentCache.Remove(key));

                // Check the keys are removed (not that all items are purged)
                // as there may have been more added during the process
                return documentCache
                    .Select(document => keysToPurge.Contains(document.Key))
                    .Count() == 0;
            }
            catch(Exception ex)
            {
                logger.LogError(ex, "Failure to purge processed items from document cache");
            }

            return false;
        }
    }
}
