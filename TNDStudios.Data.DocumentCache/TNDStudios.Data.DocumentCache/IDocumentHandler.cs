using System;

namespace TNDStudios.Data.DocumentCache
{
    public interface IDocumentHandler<T>
    {
        /// <summary>
        /// Public view of the currently connection
        /// </summary>
        string ConnectionString { get; }

        /// <summary>
        /// Database Name for the cache
        /// </summary>
        string DatabaseName { get; }

        /// <summary>
        /// Collection (table / collection / grouping) for this cache
        /// </summary>
        string DataCollection { get; }

        /// <summary>
        /// Send the object to the cache
        /// </summary>
        /// <typeparam name="T">The data type that is being sent to storage</typeparam>
        /// <param name="id">The value of the new document to be put in storage (as the json has to be case sensitive)</param>
        /// <param name="data">The data to be wrapped up in the cache document</param>
        /// <returns>Success Result</returns>
        Boolean SendToCache(string id, T data);
    }
}