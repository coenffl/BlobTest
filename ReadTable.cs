using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Cosmos.Table;

namespace coenffl.Function
{
    public static class ReadTable
    {
        [FunctionName("ReadTable")]
        public static Task<string> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]
        HttpRequest req, ILogger log, ExecutionContext context)
        {
            string connStrA = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            string requestBody = new StreamReader(req.Body).ReadToEnd();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            string PartitionKeyA = data.PartitionKey;
            string RowkeyA = data.RowKey;
            // 읽어오는 것이기 때문에 키만 있으면 된다.

            CloudStorageAccount stoA = CloudStorageAccount.Parse(connStrA);
            // 스토리지 어카운트 생성
            CloudTableClient tbC = stoA.CreateCloudTableClient();
            CloudTable tableA = tbC.GetTableReference("tableA");

            //읽어들일 쿼리
            string filterA = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThanOrEqual, PartitionKeyA);
            //파티션 키보다 크거나 같은 것을 찾아준다.
            string filterB = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, RowKeyA);
            //로우 키보다 크거나 같은 것을 찾아준다.

            Task<string> response = ReadToTable(tableA, filterA, filterB);
            return response;

        }
         
        //테이블 쿼리 생성
        static async Task<string> ReadToTable(CloudTable tableA, string filterA, string filterB)
        {
            TableQuery<MemoData> rangeQ = new TableQuery<MemoData>().Where(
               TableQuery.CombineFilters(filterA, TableOperators.And, filterB)
            );
            // 테이블 양이 너무 많을 시 제한할 양
            TableContinuationToken tokenA = null;
            rangeQ.TakeCount = 10000;
            JArray resultArr = new JArray();
            try
            {
                do 
                {
                  TableQuerySegment<MemoData> segment = await tableA.ExecuteQuerySegmentAsync(rangeQ, tokenA);
                  tokenA = segment.ContinuationToken;
                  foreach (MemoData entity in segment)
                  {
                      // 필요없는 데이터 
                      JObject srcObj = JObject.FromObject(entity);
                      srcObj.Remove("Timestamp");
                      resultArr.Add(srcObj);
                  }

                } while (tokenA != null);
            }
            catch (StorageException e)
            {

                Console.WriteLine(e.Message);
                // 디버깅 할 수 있게끔 설정
                throw;
            }

            string resultA = Newtonsoft.Json.JsonConvert.SerializeObject(resultArr);
            if(resultA != null) return resultA;
            else return "No Data";
        }

        private class MemoData: TableEntity 
        {
            public string content { get; set; }
        }
    }
}