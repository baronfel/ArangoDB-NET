using System.Collections.Generic;
using System.Threading.Tasks;
using Arango.Client;

namespace Arango.ConsoleTests
{
    public static class Database
    {
        public static string TestDatabaseOneTime { get; set; }
        public static string TestDatabaseGeneral { get; set; }

        public static string TestDocumentCollectionName { get; set; }
        public static string TestEdgeCollectionName { get; set; }

        public static string Alias { get; set; }
        public static string SystemAlias { get; set; }

        public static string Hostname { get; set; }
        public static int Port { get; set; }
        public static bool IsSecured { get; set; }
        public static string UserName { get; set; }
        public static string Password { get; set; }
        
        static Database()
        {
            TestDatabaseOneTime = "testOneTimeDatabase001xyzLatif";
            TestDatabaseGeneral = "testDatabaseGeneral001xyzLatif";

            TestDocumentCollectionName = "testDocumentCollection001xyzLatif";
            TestEdgeCollectionName = "testEdgeCollection001xyzLatif";
            
            Alias = "testAlias";
            SystemAlias = "systemAlias";
            Hostname = "localhost";
            Port = 8529;
            IsSecured = false;
            UserName = "";
            Password = "";

            ASettings.AddConnection(
                SystemAlias,
                Hostname,
                Port,
                IsSecured,
                "_system",
                UserName,
                Password
            );

            ASettings.AddConnection(
                Alias,
                Hostname,
                Port,
                IsSecured,
                TestDatabaseGeneral,
                UserName,
                Password
            );
        }
        
        public static async Task CreateTestDatabaseAsync(string databaseName)
        {	
            await DeleteTestDatabaseAsync(databaseName);

            var db = new ADatabase(Database.SystemAlias);
            
            var resultList = await db.GetAccessibleDatabasesAsync();

            if (resultList.Success && resultList.Value.Contains(databaseName))
            {
            	await db.DropAsync(databaseName);
            }
            
            await db.CreateAsync(databaseName);
        }

        public static async Task DeleteTestDatabaseAsync(string databaseName)
        {
            var db = new ADatabase(Database.SystemAlias);
            
            var resultList = await db.GetAccessibleDatabasesAsync();

            if (resultList.Success && resultList.Value.Contains(databaseName))
            {
            	await db.DropAsync(databaseName);
            }
        }

        public static async Task CleanupTestDatabasesAsync()
        {
            await DeleteTestDatabaseAsync(TestDatabaseGeneral);
            await DeleteTestDatabaseAsync(TestDatabaseOneTime);
        }
        
        public static async Task CreateTestCollectionAsync(string collectionName, ACollectionType collectionType)
        {
        	await DeleteTestCollectionAsync(collectionName);
        	
            var db = new ADatabase(Alias);

            var createResult = await db.Collection.Type(collectionType).CreateAsync(collectionName);
        }
        
        public static async Task ClearTestCollectionAsync(string collectionName)
        {
            var db = new ADatabase(Alias);

            var createResult = await db.Collection.TruncateAsync(collectionName);
        }
        
        public static async Task<List<Dictionary<string, object>>> ClearCollectionAndFetchTestDocumentDataAsync(string collectionName)
        {
            await ClearTestCollectionAsync(collectionName);
            
            var documents = new List<Dictionary<string, object>>();
        	var db = new ADatabase(Alias);
        	
        	var document1 = new Dictionary<string, object>()
        		.String("foo", "string value one")
        		.Int("bar", 1);
        	
        	var document2 = new Dictionary<string, object>()
        		.String("foo", "string value two")
        		.Int("bar", 2);
        	
        	var createResult1 = await db.Document.CreateAsync(TestDocumentCollectionName, document1);
        	
        	document1.Merge(createResult1.Value);
        	
        	var createResult2 = await db.Document.CreateAsync(TestDocumentCollectionName, document2);
        	
        	document2.Merge(createResult2.Value);
        	
        	documents.Add(document1);
        	documents.Add(document2);
        	
        	return documents;
        }

        public static async Task DeleteTestCollectionAsync(string collectionName)
        {
            var db = new ADatabase(Database.Alias);

            var resultGet = await db.Collection.GetAsync(collectionName);
            
            if (resultGet.Success && (resultGet.Value.String("name") == collectionName))
            {
                await db.Collection.DeleteAsync(collectionName);
            }
        }
    }
}
