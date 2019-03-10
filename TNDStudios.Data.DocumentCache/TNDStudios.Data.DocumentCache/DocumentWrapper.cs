using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TNDStudios.Data.DocumentCache
{
    /// <summary>
    /// Wrapping up a json based document in a format
    /// that can be handled by Cosmos (due to the case of the Id property)
    /// and that indicates if it has been processed
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [JsonObject]
    public class DocumentWrapper<T>
    {
        [JsonProperty("id")]
        public String Id { get; set; }

        [JsonProperty("data")]
        public T Data { get; set; }

        [JsonProperty("processed")]
        public Boolean Processed { get; set; }

        [JsonProperty("processedDateTime")]
        public DateTime ProcessedDateTime { get; set; }

        [JsonProperty("createdDateTime")]
        public DateTime CreatedDateTime { get; set; }
    }
}
