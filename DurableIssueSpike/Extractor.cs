using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DXCDurableSpike
{
    public static class Extractor
    {
        [FunctionName("ClientStart")]
        public static async Task<HttpResponseMessage> ClientStart(
            [HttpTrigger] HttpRequestMessage req,
            [OrchestrationClient]DurableOrchestrationClient starter,
            TraceWriter log)
        {
            log.Info($"Start: {DateTime.UtcNow.ToLongDateString()}");
            var instanceId = await starter.StartNewAsync("Orchestrator", null);
            var result = JsonConvert.SerializeObject(new JObject { ["instanceId"] = instanceId });
            return new HttpResponseMessage()
            {
                Content = new StringContent(result, System.Text.Encoding.UTF8, "application/text")
            };
        }

        [FunctionName("ClientStop")]
        public static async Task<HttpResponseMessage> ClientTerminator(
            [HttpTrigger] HttpRequestMessage req,
            [OrchestrationClient] DurableOrchestrationClient terminator,
            TraceWriter log)
        {
            var body = await req.Content.ReadAsStringAsync();
            var restored = JsonConvert.DeserializeObject<JObject>(body);
            var instanceId = restored["instanceId"].Value<string>();

            await terminator.TerminateAsync(instanceId, "Stop command fires.");
            log.Info($"Stopped InstanceId: {instanceId}");
            var result = JsonConvert.SerializeObject(new JObject { ["instanceId"] = $"stop {instanceId}" });
            return new HttpResponseMessage()
            {
                Content = new StringContent(result, System.Text.Encoding.UTF8, "application/text")
            };
        }

        [FunctionName("ClientStatus")]
        public static async Task<HttpResponseMessage> ClientStatus(
     [HttpTrigger] HttpRequestMessage req,
     [OrchestrationClient] DurableOrchestrationClient client,
     TraceWriter log)
        {
            var body = await req.Content.ReadAsStringAsync();
            var restored = JsonConvert.DeserializeObject<JObject>(body);
            var status = await client.GetStatusAsync(restored["instanceId"].ToString());


            var result = $"clients status. -> {JsonConvert.SerializeObject(status)}";
            return new HttpResponseMessage()
            {
                Content = new StringContent(result, System.Text.Encoding.UTF8, "application/text")
            };
        }


        [FunctionName("Orchestrator")]
        public static async Task Orchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context)
        {
            await context.CallSubOrchestratorAsync("ExtractOrchestrator", null);
            var nextSchedule = context.CurrentUtcDateTime.AddSeconds(60); // It is going to be 10 min. However for testing.
            await context.CreateTimer(nextSchedule, CancellationToken.None);
            context.ContinueAsNew(null);
        }

        [FunctionName("ExtractOrchestrator")]
        public static async Task ExtractOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context)
        {
            const int NUMBER_OF_TABLES = 2;
            var tasks = new Task[NUMBER_OF_TABLES];
            tasks[0] = context.CallActivityAsync("BugExtractor", "execute");
            tasks[1] = context.CallActivityAsync("CycleExtractor", "execute");
            await Task.WhenAll(tasks);
        }

        [FunctionName("BugExtractor")]
        public static async Task BugExtractor(
            [ActivityTrigger] string input, TraceWriter log, IBinder binder)
        { 
                log.Info($"*********BugExtractor is executed********");
        }

        [FunctionName("CycleExtractor")]
        public static async Task CycleExtractor(
        [ActivityTrigger] string input, TraceWriter log)
        {
            log.Info($"*********CycleExtractor is executed********");
        }
    }
}
