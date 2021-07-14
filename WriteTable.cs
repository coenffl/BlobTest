using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Cosmos.Table;

namespace coenffl.Function
{
    public static class WriteTable
    {
        [FunctionName("WriteTable")]
        public static void Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]
        HttpRequest req, ILogger log, ExecutionContext context)
        {
            string connStrA = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            string requestBody = new StreamReader(req.Body).ReadToEnd();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            string PartitionKeyA = data.PartitionKey;
            string RowkeyA = data.RowKey;
            string contentA = data.Content;
            // 여기서 데이터를 받음

            CloudStorageAccount stoA = CloudStorageAccount.Parse(connStrA);
            // 스토리지 어카운트 생성
            CloudTableClient tbC = stoA.CreateCloudTableClient();
            CloudTable tableA = tbC.GetTableReference("tableA");

            writeToTable(tableA, contentA, PartitionKeyA, RowkeyA);

        }

        // 함수가 실행될 때 데이터를 받아옴 
        static void writeToTable(CloudTable tableA, string contentA, string PartitionKeyA, string RowKeyA)
        // JSON 형태로 받아온 데이터를 모두 받아온다.
        {
            MemoData memoA = new MemoData();
            memoA.PartitionKey = PartitionKeyA;
            memoA.RowKey = RowKeyA;
            memoA.content = contentA;

            TableOperation operA = TableOperation.InsertOrReplace(memoA);
            tableA.Execute(operA);
        }

        private class MemoData: TableEntity 
        // 키를 따로 적지 않아도 TableEntity에 들어있다.
        {
            public string content { get; set; }
        }
    }
}