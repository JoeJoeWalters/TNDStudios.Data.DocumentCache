using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

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
        private List<T> documentCache;

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
        /// Send the object to the cache
        /// </summary>
        /// <typeparam name="T">The data type that is being sent to storage</typeparam>
        /// <param name="id">The value of the new document to be put in storage (as the json has to be case sensitive)</param>
        /// <param name="data">The data to be wrapped up in the cache document</param>
        /// <returns>Success Result</returns>
        public bool Save(string id, T data)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Get the object from the cache by the id reference
        /// </summary>
        /// <param name="id">The id of the document</param>
        /// <returns>The document from the cache</returns>
        public T Get(string id) { throw new NotImplementedException(); }

        /// <summary>
        /// Get all items that are marked a having not been processed yet
        /// </summary>
        /// <returns>A list of unprocessed items</returns>
        public List<T> GetToProcess(Int32 maxRecords) { throw new NotImplementedException(); }

        /// <summary>
        /// Mark a set of documents as processed
        /// </summary>
        /// <param name="documentsToMark">A list of document id's to mark as processed</param>
        /// <returns>The id's of the documents that did get marked</returns>
        public List<String> MarkAsProcessed(List<String> documentsToMark) { throw new NotImplementedException(); }

        /// <summary>
        /// Purge (Delete) all items marked as processed
        /// </summary>
        /// <returns>If the purge was successful</returns>
        public Boolean Purge() { throw new NotImplementedException(); }
    }
}
