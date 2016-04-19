using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using PreProcessing_VM.Models;
using System.Diagnostics;
using System.Configuration;
//submitting hadoop jobs to hdinsight
using System.Security.Cryptography.X509Certificates;
using Microsoft.WindowsAzure.Management.HDInsight;
using System.Xml.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Web.Configuration;
//For Ambari Monitoring Client
using Microsoft.Hadoop.WebClient.AmbariClient;
using Microsoft.Hadoop.WebClient.AmbariClient.Contracts;
//For Regex
using System.Text.RegularExpressions;
//For thread
using System.Threading;
//For Blob Storage
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Hadoop.Client;
using System.IO;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure;



namespace PreProcessing_VM.Controllers
{
    public class HomeController : Controller
    {
        
        public ActionResult Index()
        {
            Trace.WriteLine("Loading Home Page...");
            Trace.TraceInformation("Displaying the Home page at "+ DateTime.Now.ToLongTimeString());
            Trace.WriteLine("Leaving Home Page...");
            return View();
        }

        
        /*
        public FileStreamResult SaveData(string example)
        {

            Trace.WriteLine("Calling SaveData method...");
            Trace.TraceInformation("Saving Data To File at " + DateTime.Now.ToLongTimeString());

            //todo: add some data from your database into that string:
            var string_with_your_data = example;
            
            //Build your stream
            var byteArray = Encoding.ASCII.GetBytes(string_with_your_data);
            var stream = new MemoryStream(byteArray);

            Trace.WriteLine("Data Saved To File.");

            //Returns a file that will match your value passed in (ie output.txt)
            return File(stream, "text/plain", "output.txt");

            
        }*/

        [HttpGet]
        public ActionResult EnterQuery()
        {
            Trace.WriteLine("Entering EnterQuery[Get] method");
            Trace.TraceInformation("Displaying the EnterQuery[Get] page at " + DateTime.Now.ToLongTimeString());
            Trace.WriteLine("Leaving EnterQuery[Get] method");

            return View();
        }

        [HttpPost]
        public ActionResult EnterQuery(Query q1)
        {
            Trace.WriteLine("Entering EnterQuery[Post] method");
            Trace.TraceInformation("Displaying the EnterQuery[Post] page at " + DateTime.Now.ToLongTimeString());

            if (q1.HiveQuery != null)
                return RedirectToAction("Execute", new { q = q1.HiveQuery });

            Trace.WriteLine("Leaving EnterQuery[Post] method");
            return View();
        }




        public async Task<ActionResult> Execute(string q)
        {
            Trace.WriteLine("Entering Execute method");
            Trace.TraceInformation("Displaying the Execute page at " + DateTime.Now.ToLongTimeString());

            string output="";
            var call = Task.Factory.StartNew(() => output = HiveOutput(q));
            await call;

            Trace.WriteLine("Leaving Execute method");

            return View((object)output);

        }

        //Helper Function to Wait while job executes
        private static void WaitForJobCompletion(JobCreationResults jobResults, IJobSubmissionClient client)
        {
            Trace.WriteLine("Entering WaitForJobCompletion method");
            Trace.TraceInformation("Executing WaitForJobCompletion method " + DateTime.Now.ToLongTimeString());

            JobDetails jobInProgress = client.GetJob(jobResults.JobId);
            while (jobInProgress.StatusCode != JobStatusCode.Completed &&
            jobInProgress.StatusCode != JobStatusCode.Failed)
            {
                jobInProgress = client.GetJob(jobInProgress.JobId);
                Thread.Sleep(TimeSpan.FromSeconds(1));
                Console.Write(".");
            }
            Trace.WriteLine("Leaving WaitForJobCompletion method");
        }

        public string HiveOutput(string q)
        {
            Trace.WriteLine("Entering HiveOutput method");
            Trace.TraceInformation("Executing HiveOutput method at " + DateTime.Now.ToLongTimeString());

            //Defining MapReduce Job
            HiveJobCreateParameters hiveJobDefinition = new HiveJobCreateParameters()
            {
                JobName = "job",
                StatusFolder = "/TableListFolder",
                Query = q
            };

            
            Guid subscriptionId = new Guid("44fbb137-edbb-4044-9db9-0e1333e137cf");     //your-subscription-id
            string clusterName = "tbanihumcluster";

            // Get the certificate object from certificate store using the friendly name to identify it
            X509Store store = new X509Store(StoreName.My);
            store.Open(OpenFlags.ReadOnly);
            X509Certificate2 cert = store.Certificates.Cast<X509Certificate2>().First(item => item.FriendlyName == "Azdem187U23713U-1-8-2015-credentials");
            var creds = new JobSubmissionCertificateCredential(subscriptionId, cert, clusterName);
            
            // Create a hadoop client to connect to HDInsight
            var jobClient = JobSubmissionClientFactory.Connect(creds);

            // Run the MapReduce job
            JobCreationResults jobResults = jobClient.CreateHiveJob(hiveJobDefinition);

            // Wait for the job to complete
            WaitForJobCompletion(jobResults, jobClient);
            
            // Hive job output
            System.IO.Stream stream = jobClient.GetJobOutput(jobResults.JobId);
            System.IO.StreamReader reader = new System.IO.StreamReader(stream);
            string value = reader.ReadToEnd();

            value = value.Replace('\t', ',');

            
            // Retrieve storage account from connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.GetContainerReference("mycontainer");
            container.CreateIfNotExists();

            // Retrieve reference to a blob named "myblob".
            CloudBlockBlob blockBlob = container.GetBlockBlobReference("myblob");
            blockBlob.UploadText(value);

            
            Trace.WriteLine("Leaving HiveOutput method");

            return value;
        }

        public ActionResult About()
        {
            Trace.WriteLine("Entering About method");
            Trace.TraceInformation("Displaying the About page at " + DateTime.Now.ToLongTimeString());
            Trace.WriteLine("Leaving About method");

            return View();
        }

        public ActionResult Contact()
        {
            Trace.WriteLine("Entering Contact method");
            Trace.TraceInformation("Displaying the Contact page at " + DateTime.Now.ToLongTimeString());
            Trace.WriteLine("Leaving Contact method");

            return View();
        }


        public async Task<ActionResult> List()
        {
            Trace.WriteLine("Entering List method");
            Trace.TraceInformation("Executing List method at " + DateTime.Now.ToLongTimeString());

            ViewBag.Message = "Your application description page.";
            var c = new List<ClusterList>();
            var call = Task.Factory.StartNew(() => c = GetAsync());
            //List<ClusterList> cluster = GetAsync();
            await call;

            Trace.WriteLine("Leaving List method");

            return View(c);
        }


        public List<ClusterList> GetAsync()
        {
            Trace.WriteLine("Entering GetAsync method");
            Trace.TraceInformation("Executing GetAsync method at " + DateTime.Now.ToLongTimeString());


            Guid subscriptionId = new Guid("44fbb137-edbb-4044-9db9-0e1333e137cf");     //your-subscription-id
            string certName = "Azdem187U23713U-1-8-2015-credentials";                   //your-subscription-management-cert-name

            // Create an HDInsight Client
            X509Store store = new X509Store(StoreName.My);
            store.Open(OpenFlags.ReadOnly);
            X509Certificate2 cert = store.Certificates.Cast<X509Certificate2>().Single(item => item.FriendlyName == certName); //your friendly name
            HDInsightCertificateCredential creds = new HDInsightCertificateCredential(subscriptionId, cert);
            IHDInsightClient client = HDInsightClient.Connect(creds);

            //var c =Task.Run(() => client.ListClusters()).Result;
            var cluster = client.ListClusters();

            var c1 = new List<ClusterList>();
            foreach (var item in cluster)
            {
                c1.Add(new ClusterList() { Name = item.Name, Node = item.ClusterSizeInNodes});
            }

            Trace.WriteLine("Leaving GetAsync method");
            return c1;
        }


        public ActionResult Loggedin()
        {
            Trace.WriteLine("Entering Loggedin method");
            Trace.TraceInformation("Displaying the Loggedin page at " + DateTime.Now.ToLongTimeString());
            Trace.WriteLine("Leaving Loggedin method");

            return View();
        }




        public ActionResult LearnMore()
        {
            Trace.WriteLine("Entering LearnMore method");
            Trace.TraceInformation("Displaying the LearnMore page at " + DateTime.Now.ToLongTimeString());
            Trace.WriteLine("Leaving LearnMore method");

            return View();
        }

        public EmptyResult Download()
        {
            String name = "myblob";
            // Retrieve storage account from connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.GetContainerReference("mycontainer");

            // force download
            Response.AddHeader("Content-Disposition", "attachment; filename=" + name); 

            container.GetBlockBlobReference(name).DownloadToStream(Response.OutputStream);
            
            return new EmptyResult();


        }

        public ActionResult DownloadBlob()
        {
            Download();
            return View();
        }
    }
}