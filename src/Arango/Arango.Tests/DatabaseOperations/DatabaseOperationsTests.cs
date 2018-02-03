using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Arango.Client;
using System.Threading.Tasks;

namespace Arango.Tests
{
    [TestFixture()]
    public class DatabaseOperationsTests : IDisposable
    {
        [Test()]
        public async Task Should_get_list_of_accessible_databases()
        {
            await Database.CleanupTestDatabasesAsync();
        	
            var db = new ADatabase(Database.SystemAlias);

            var resultCreate = await db.CreateAsync(Database.TestDatabaseOneTime);

            var resultList = await db.GetAccessibleDatabasesAsync();

            Assert.AreEqual(200, resultList.StatusCode);
            Assert.IsTrue(resultList.Success);
            Assert.IsTrue(resultList.HasValue);
            Assert.IsTrue(resultList.Value.Contains(Database.TestDatabaseOneTime));
        }
        
        [Test()]
        public async Task Should_get_list_of_all_databases()
        {
            await Database.CleanupTestDatabasesAsync();
        	
            var db = new ADatabase(Database.SystemAlias);

            var resultCreate = await db.CreateAsync(Database.TestDatabaseOneTime);

            var resultList = await db.GetAllDatabasesAsync();

            Assert.AreEqual(200, resultList.StatusCode);
            Assert.IsTrue(resultList.Success);
            Assert.IsTrue(resultList.HasValue);
            Assert.IsTrue(resultList.Value.Contains(Database.TestDatabaseOneTime));
            Assert.IsTrue(resultList.Value.Contains("_system"));
        }
        
        [Test()]
        public async Task Should_get_current_database()
        {
            await Database.CleanupTestDatabasesAsync();

            var db = new ADatabase(Database.SystemAlias);

            var resultCurrent = await db.GetCurrentAsync();

            Assert.AreEqual(200, resultCurrent.StatusCode);
            Assert.IsTrue(resultCurrent.Success);
            Assert.IsTrue(resultCurrent.HasValue);
            Assert.AreEqual("_system", resultCurrent.Value.String("name"));
            Assert.AreEqual(false, string.IsNullOrEmpty(resultCurrent.Value.String("id")));
            Assert.AreEqual(false, string.IsNullOrEmpty(resultCurrent.Value.String("path")));
            Assert.AreEqual(true, resultCurrent.Value.Bool("isSystem"));
        }
        
        [Test()]
        public async Task Should_get_database_collections()
        {
            await Database.CleanupTestDatabasesAsync();
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);

            var db = new ADatabase(Database.Alias);

            var createResult = await db.Collection
                .CreateAsync(Database.TestDocumentCollectionName);

            var getResult = await db
                .ExcludeSystem(true)
                .GetAllCollectionsAsync();
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            
            var foundCreatedCollection = getResult.Value.FirstOrDefault(col => col.String("name") == createResult.Value.String("name"));
            
            Assert.IsNotNull(foundCreatedCollection);
            
            var foundSystemCollection = getResult.Value.FirstOrDefault(col => col.String("name") == "_system");
            
            Assert.IsNull(foundSystemCollection);
        }
        
        [Test()]
        public async Task Should_create_database()
        {
            await Database.CleanupTestDatabasesAsync();
        	
            var db = new ADatabase(Database.SystemAlias);

            var createResult = await db.CreateAsync(Database.TestDatabaseOneTime);

            Assert.AreEqual(201, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            Assert.IsTrue(createResult.HasValue);
            Assert.IsTrue(createResult.Value);
        }
        
        [Test()]
        public async Task Should_create_database_with_users()
        {
            await Database.CleanupTestDatabasesAsync();
        	
            var db = new ADatabase(Database.SystemAlias);

            var users = new List<AUser>()
            {
                new AUser { Username = "admin", Password = "secret", Active = true },
                new AUser { Username = "tester001", Password = "test001", Active = false } 
            };
            
            var createResult = await db.CreateAsync(Database.TestDatabaseOneTime, users);

            Assert.AreEqual(201, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            Assert.IsTrue(createResult.HasValue);
            Assert.IsTrue(createResult.Value);
        }
        
        [Test()]
        public async Task Should_fail_create_already_existing_database()
        {
            await Database.CleanupTestDatabasesAsync();
        	
            var db = new ADatabase(Database.SystemAlias);

            var createResult = await db.CreateAsync(Database.TestDatabaseGeneral);
            
            var createResult2 = await db.CreateAsync(Database.TestDatabaseGeneral);

            Assert.AreEqual(409, createResult2.StatusCode);
            Assert.IsFalse(createResult2.Success);
            Assert.IsTrue(createResult2.HasValue);
            Assert.IsFalse(createResult2.Value);
            Assert.IsNotNull(createResult2.Error);
        }
        
        [Test()]
        public async Task Should_fail_create_database_from_non_system_database()
        {
            await Database.CleanupTestDatabasesAsync();
        	
            var db = new ADatabase(Database.SystemAlias);

            var createResult = await db.CreateAsync(Database.TestDatabaseGeneral);

            var nonSystemDatabase = new ADatabase(Database.Alias);
            
            var createResult2 = await nonSystemDatabase.CreateAsync(Database.TestDatabaseOneTime);

            Assert.AreEqual(403, createResult2.StatusCode);
            Assert.IsFalse(createResult2.Success);
            Assert.IsTrue(createResult2.HasValue);
            Assert.IsFalse(createResult2.Value);
            Assert.IsNotNull(createResult2.Error);
        }
        
        [Test()]
        public async Task Should_delete_database()
        {
            await Database.CleanupTestDatabasesAsync();
        	
            var db = new ADatabase(Database.SystemAlias);

            var createResult = await db.CreateAsync(Database.TestDatabaseGeneral);
            
            var deleteResult = await db.DropAsync(Database.TestDatabaseGeneral);

            Assert.AreEqual(200, deleteResult.StatusCode);
            Assert.IsTrue(deleteResult.Success);
            Assert.IsTrue(deleteResult.HasValue);
            Assert.IsTrue(deleteResult.Value);
        }
        
        public void Dispose()
        {
            Database.CleanupTestDatabasesAsync().Wait();
        }
    }
}
