using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Arango.Client;
using System.Threading.Tasks;

namespace Arango.Tests
{
    [TestFixture()]
    public class CollectionOperationsTests : IDisposable
    {
        #region Create operations
    	
        [Test()]
        public async Task Should_create_document_collectionAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);

            var db = new ADatabase(Database.Alias);

            var createResult = await
                db.Collection.CreateAsync(Database.TestDocumentCollectionName);

            Assert.AreEqual(200, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            Assert.IsTrue(createResult.HasValue);
            Assert.AreEqual(true, createResult.Value.IsString("id"));
            Assert.AreEqual(Database.TestDocumentCollectionName, createResult.Value.String("name"));
            Assert.AreEqual(false, createResult.Value.Bool("waitForSync"));
            Assert.AreEqual(false, createResult.Value.Bool("isVolatile"));
            Assert.AreEqual(false, createResult.Value.Bool("isSystem"));
            Assert.AreEqual(ACollectionStatus.Loaded, createResult.Value.Enum<ACollectionStatus>("status"));
            Assert.AreEqual(ACollectionType.Document, createResult.Value.Enum<ACollectionType>("type"));
        }
        
        [Test()]
        public async Task Should_create_edge_collectionAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);

            var db = new ADatabase(Database.Alias);

            var createResult = await db.Collection
                .Type(ACollectionType.Edge)
                .CreateAsync(Database.TestEdgeCollectionName);

            Assert.AreEqual(200, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            Assert.IsTrue(createResult.HasValue);
            Assert.AreEqual(true, createResult.Value.IsString("id"));
            Assert.AreEqual(Database.TestEdgeCollectionName, createResult.Value.String("name"));
            Assert.AreEqual(false, createResult.Value.Bool("waitForSync"));
            Assert.AreEqual(false, createResult.Value.Bool("isVolatile"));
            Assert.AreEqual(false, createResult.Value.Bool("isSystem"));
            Assert.AreEqual(ACollectionStatus.Loaded, createResult.Value.Enum<ACollectionStatus>("status"));
            Assert.AreEqual(ACollectionType.Edge, createResult.Value.Enum<ACollectionType>("type"));
        }
        
        [Test()]
        public async Task Should_create_autoincrement_collectionAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);
            
            var db = new ADatabase(Database.Alias);

            var createResult = await db.Collection
                .KeyGeneratorType(AKeyGeneratorType.Autoincrement)
                .CreateAsync(Database.TestDocumentCollectionName);
            
            Assert.AreEqual(200, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            Assert.IsTrue(createResult.HasValue);
            Assert.AreEqual(true, createResult.Value.IsString("id"));
            Assert.AreEqual(Database.TestDocumentCollectionName, createResult.Value.String("name"));
            Assert.AreEqual(false, createResult.Value.Bool("waitForSync"));
            Assert.AreEqual(false, createResult.Value.Bool("isVolatile"));
            Assert.AreEqual(false, createResult.Value.Bool("isSystem"));
            Assert.AreEqual(ACollectionStatus.Loaded, createResult.Value.Enum<ACollectionStatus>("status"));
            Assert.AreEqual(ACollectionType.Document, createResult.Value.Enum<ACollectionType>("type"));


			// create documents and test if their key are incremented accordingly
			
            var newDocument = new Dictionary<string, object>()
                .String("foo", "some string")
                .Document("bar", new Dictionary<string, object>().String("foo", "string value"));
            
            var doc1Result = await db.Document
                .CreateAsync(Database.TestDocumentCollectionName, newDocument);
            
            Assert.AreEqual(202, doc1Result.StatusCode);
            Assert.IsTrue(doc1Result.Success);
            Assert.IsTrue(doc1Result.HasValue);
            Assert.AreEqual(Database.TestDocumentCollectionName + "/" + 1, doc1Result.Value.ID());
            Assert.AreEqual("1", doc1Result.Value.Key());
            Assert.IsFalse(string.IsNullOrEmpty(doc1Result.Value.Rev()));
            
            var doc2Result = await db.Document
                .CreateAsync(Database.TestDocumentCollectionName, newDocument);
            
            Assert.AreEqual(202, doc2Result.StatusCode);
            Assert.IsTrue(doc2Result.Success);
            Assert.IsTrue(doc2Result.HasValue);
            Assert.AreEqual(Database.TestDocumentCollectionName + "/" + 2, doc2Result.Value.ID());
            Assert.AreEqual("2", doc2Result.Value.Key());
            Assert.IsFalse(string.IsNullOrEmpty(doc2Result.Value.Rev()));
        }
        
        #endregion
        
        #region Get operations
        
        [Test()]
        public async Task Should_get_collectionAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);

            var db = new ADatabase(Database.Alias);

            var createResult = await db.Collection.CreateAsync(Database.TestDocumentCollectionName);

            var getResult = await db.Collection.GetAsync(createResult.Value.String("name"));
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(createResult.Value.String("id"), getResult.Value.String("id"));
            Assert.AreEqual(createResult.Value.String("name"), getResult.Value.String("name"));
            Assert.AreEqual(createResult.Value.Bool("isSystem"), getResult.Value.Bool("isSystem"));
            Assert.AreEqual(createResult.Value.Int("status"), getResult.Value.Int("status"));
            Assert.AreEqual(createResult.Value.Int("type"), getResult.Value.Int("type"));
        }
        
        [Test()]
        public async Task Should_get_collection_propertiesAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);

            var db = new ADatabase(Database.Alias);

            var createResult = await db.Collection
                .CreateAsync(Database.TestDocumentCollectionName);

            var getResult = await db.Collection
                .GetPropertiesAsync(createResult.Value.String("name"));
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(createResult.Value.String("id"), getResult.Value.String("id"));
            Assert.AreEqual(createResult.Value.String("name"), getResult.Value.String("name"));
            Assert.AreEqual(createResult.Value.Bool("isSystem"), getResult.Value.Bool("isSystem"));
            Assert.AreEqual(createResult.Value.Bool("isVolatile"), getResult.Value.Bool("isVolatile"));
            Assert.AreEqual(createResult.Value.Int("status"), getResult.Value.Int("status"));
            Assert.AreEqual(createResult.Value.Int("type"), getResult.Value.Int("type"));
            Assert.AreEqual(createResult.Value.Bool("waitForSync"), getResult.Value.Bool("waitForSync"));
            Assert.IsTrue(getResult.Value.Bool("doCompact"));
            Assert.IsTrue(getResult.Value.Long("journalSize") > 1);
            Assert.AreEqual(AKeyGeneratorType.Traditional, getResult.Value.Enum<AKeyGeneratorType>("keyOptions.type"));
            Assert.AreEqual(true, getResult.Value.Bool("keyOptions.allowUserKeys"));
        }
        
        [Test()]
        public async Task Should_get_collection_countAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);

            var db = new ADatabase(Database.Alias);

            var createResult = await db.Collection
                .CreateAsync(Database.TestDocumentCollectionName);

            var getResult = await db.Collection
                .GetCountAsync(createResult.Value.String("name"));
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(createResult.Value.String("id"), getResult.Value.String("id"));
            Assert.AreEqual(createResult.Value.String("name"), getResult.Value.String("name"));
            Assert.AreEqual(createResult.Value.Bool("isSystem"), getResult.Value.Bool("isSystem"));
            Assert.AreEqual(createResult.Value.Bool("isVolatile"), getResult.Value.Bool("isVolatile"));
            Assert.AreEqual(createResult.Value.Int("status"), getResult.Value.Int("status"));
            Assert.AreEqual(createResult.Value.Int("type"), getResult.Value.Int("type"));
            Assert.AreEqual(createResult.Value.Bool("waitForSync"), getResult.Value.Bool("waitForSync"));
            Assert.IsTrue(getResult.Value.Bool("doCompact"));
            Assert.IsTrue(getResult.Value.Long("journalSize") > 1);
            Assert.AreEqual(AKeyGeneratorType.Traditional, getResult.Value.Enum<AKeyGeneratorType>("keyOptions.type"));
            Assert.AreEqual(true, getResult.Value.Bool("keyOptions.allowUserKeys"));
            Assert.AreEqual(0, getResult.Value.Long("count"));
        }
        
        [Test()]
        public async Task Should_get_collection_figuresAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);

            var db = new ADatabase(Database.Alias);

            var createResult = await db.Collection
                .CreateAsync(Database.TestDocumentCollectionName);

            var getResult = await db.Collection
                .GetFiguresAsync(createResult.Value.String("name"));
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(createResult.Value.String("id"), getResult.Value.String("id"));
            Assert.AreEqual(createResult.Value.String("name"), getResult.Value.String("name"));
            Assert.AreEqual(createResult.Value.Bool("isSystem"), getResult.Value.Bool("isSystem"));
            Assert.AreEqual(createResult.Value.Bool("isVolatile"), getResult.Value.Bool("isVolatile"));
            Assert.AreEqual(createResult.Value.Int("status"), getResult.Value.Int("status"));
            Assert.AreEqual(createResult.Value.Int("type"), getResult.Value.Int("type"));
            Assert.AreEqual(createResult.Value.Bool("waitForSync"), getResult.Value.Bool("waitForSync"));
            Assert.IsTrue(getResult.Value.Bool("doCompact"));
            Assert.IsTrue(getResult.Value.Long("journalSize") > 0);
            Assert.AreEqual(AKeyGeneratorType.Traditional, getResult.Value.Enum<AKeyGeneratorType>("keyOptions.type"));
            Assert.AreEqual(true, getResult.Value.Bool("keyOptions.allowUserKeys"));
            Assert.AreEqual(0, getResult.Value.Long("count"));
            Assert.IsTrue(getResult.Value.Document("figures").Count > 0);
        }
        
        [Test()]
        public async Task Should_get_collection_revisionAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);

            var db = new ADatabase(Database.Alias);

            var createResult = await db.Collection
                .CreateAsync(Database.TestDocumentCollectionName);

            var getResult = await db.Collection
                .GetRevisionAsync(createResult.Value.String("name"));
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(createResult.Value.String("id"), getResult.Value.String("id"));
            Assert.AreEqual(createResult.Value.String("name"), getResult.Value.String("name"));
            Assert.AreEqual(createResult.Value.Bool("isSystem"), getResult.Value.Bool("isSystem"));
            Assert.AreEqual(createResult.Value.Int("status"), getResult.Value.Int("status"));
            Assert.AreEqual(createResult.Value.Int("type"), getResult.Value.Int("type"));
            Assert.IsTrue(getResult.Value.IsString("revision"));
        }
        
        [Test()]
        public async Task Should_get_collection_cehcksumAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);

            var db = new ADatabase(Database.Alias);

            var createResult = await db.Collection
                .CreateAsync(Database.TestDocumentCollectionName);

            var getResult = await db.Collection
                .WithData(true)
                .WithRevisions(true)
                .GetChecksumAsync(createResult.Value.String("name"));
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(createResult.Value.String("id"), getResult.Value.String("id"));
            Assert.AreEqual(createResult.Value.String("name"), getResult.Value.String("name"));
            Assert.AreEqual(createResult.Value.Bool("isSystem"), getResult.Value.Bool("isSystem"));
            Assert.AreEqual(createResult.Value.Int("status"), getResult.Value.Int("status"));
            Assert.AreEqual(createResult.Value.Int("type"), getResult.Value.Int("type"));
            Assert.IsTrue(getResult.Value.IsString("revision"));
            Assert.IsTrue(getResult.Value.IsString("checksum"));
        }
        
        [Test()]
        public async Task Should_get_all_indexes_in_collectionAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);
            await Database.CreateTestCollectionAsync(Database.TestDocumentCollectionName, ACollectionType.Document);
            var db = new ADatabase(Database.Alias);
            
            var operationResult = await db.Collection
                .GetAllIndexesAsync(Database.TestDocumentCollectionName);
            
            Assert.AreEqual(200, operationResult.StatusCode);
            Assert.IsTrue(operationResult.Success);
            Assert.IsTrue(operationResult.HasValue);
            Assert.IsTrue(operationResult.Value.Size("indexes") > 0);
            Assert.IsTrue(operationResult.Value.IsDocument("identifiers"));
        }
        
        #endregion
        
        #region Update/change operations
        
        [Test()]
        public async Task Should_truncate_collectionAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);

            var db = new ADatabase(Database.Alias);

            var createResult = await db.Collection
                .CreateAsync(Database.TestDocumentCollectionName);

            var clearResult = await db.Collection
                .TruncateAsync(createResult.Value.String("name"));
            
            Assert.AreEqual(200, clearResult.StatusCode);
            Assert.IsTrue(clearResult.Success);
            Assert.IsTrue(clearResult.HasValue);
            Assert.AreEqual(createResult.Value.String("id"), clearResult.Value.String("id"));
            Assert.AreEqual(createResult.Value.String("name"), clearResult.Value.String("name"));
            Assert.AreEqual(createResult.Value.Bool("isSystem"), clearResult.Value.Bool("isSystem"));
            Assert.AreEqual(createResult.Value.Int("status"), clearResult.Value.Int("status"));
            Assert.AreEqual(createResult.Value.Int("type"), clearResult.Value.Int("type"));
        }
        
        [Test()]
        public async Task Should_load_collectionAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);

            var db = new ADatabase(Database.Alias);

            var createResult = await db.Collection
                .CreateAsync(Database.TestDocumentCollectionName);

            var operationResult = await db.Collection
                .LoadAsync(createResult.Value.String("name"));
            
            Assert.AreEqual(200, operationResult.StatusCode);
            Assert.IsTrue(operationResult.Success);
            Assert.IsTrue(operationResult.HasValue);
            Assert.AreEqual(createResult.Value.String("id"), operationResult.Value.String("id"));
            Assert.AreEqual(createResult.Value.String("name"), operationResult.Value.String("name"));
            Assert.AreEqual(createResult.Value.Bool("isSystem"), operationResult.Value.Bool("isSystem"));
            Assert.AreEqual(createResult.Value.Int("status"), operationResult.Value.Int("status"));
            Assert.AreEqual(createResult.Value.Int("type"), operationResult.Value.Int("type"));
            Assert.IsTrue(operationResult.Value.Long("count") == 0);
        }
        
        [Test()]
        public async Task Should_load_collection_without_countAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);

            var db = new ADatabase(Database.Alias);

            var createResult = await db.Collection
                .CreateAsync(Database.TestDocumentCollectionName);

            var operationResult = await db.Collection
                .Count(false)
                .LoadAsync(createResult.Value.String("name"));
            
            Assert.AreEqual(200, operationResult.StatusCode);
            Assert.IsTrue(operationResult.Success);
            Assert.IsTrue(operationResult.HasValue);
            Assert.AreEqual(createResult.Value.String("id"), operationResult.Value.String("id"));
            Assert.AreEqual(createResult.Value.String("name"), operationResult.Value.String("name"));
            Assert.AreEqual(createResult.Value.Bool("isSystem"), operationResult.Value.Bool("isSystem"));
            Assert.AreEqual(ACollectionStatus.Loaded, operationResult.Value.Enum<ACollectionStatus>("status"));
            Assert.AreEqual(createResult.Value.Int("type"), operationResult.Value.Int("type"));
            Assert.IsFalse(operationResult.Value.Has("count"));
        }
        
        [Test()]
        public async Task Should_unload_collectionAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);

            var db = new ADatabase(Database.Alias);

            var createResult = await db.Collection
                .CreateAsync(Database.TestDocumentCollectionName);

            var operationResult = await db.Collection
                .UnloadAsync(createResult.Value.String("name"));
            
            Assert.AreEqual(200, operationResult.StatusCode);
            Assert.IsTrue(operationResult.Success);
            Assert.IsTrue(operationResult.HasValue);
            Assert.AreEqual(createResult.Value.String("id"), operationResult.Value.String("id"));
            Assert.AreEqual(createResult.Value.String("name"), operationResult.Value.String("name"));
            Assert.AreEqual(createResult.Value.Bool("isSystem"), operationResult.Value.Bool("isSystem"));
            Assert.IsTrue(operationResult.Value.Enum<ACollectionStatus>("status") == ACollectionStatus.Unloaded || operationResult.Value.Enum<ACollectionStatus>("status") == ACollectionStatus.Unloading);
            Assert.AreEqual(createResult.Value.Int("type"), operationResult.Value.Int("type"));
        }
        
        [Test()]
        public async Task Should_change_collection_propertiesAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);

            var db = new ADatabase(Database.Alias);

            var createResult = await db.Collection
                .CreateAsync(Database.TestDocumentCollectionName);

            const long journalSize = 199999999;
            
            var operationResult = await db.Collection
                .WaitForSync(true)
                .JournalSize(journalSize)
                .ChangePropertiesAsync(createResult.Value.String("name"));
            
            Assert.AreEqual(200, operationResult.StatusCode);
            Assert.IsTrue(operationResult.Success);
            Assert.IsTrue(operationResult.HasValue);
            Assert.AreEqual(createResult.Value.String("id"), operationResult.Value.String("id"));
            Assert.AreEqual(createResult.Value.String("name"), operationResult.Value.String("name"));
            Assert.AreEqual(createResult.Value.Bool("isSystem"), operationResult.Value.Bool("isSystem"));
            Assert.AreEqual(createResult.Value.Int("status"), operationResult.Value.Int("status"));
            Assert.AreEqual(createResult.Value.Int("type"), operationResult.Value.Int("type"));
            Assert.IsFalse(operationResult.Value.Bool("isVolatile"));
            Assert.IsTrue(operationResult.Value.Bool("doCompact"));
            Assert.AreEqual(AKeyGeneratorType.Traditional, operationResult.Value.Enum<AKeyGeneratorType>("keyOptions.type"));
            Assert.IsTrue(operationResult.Value.Bool("keyOptions.allowUserKeys"));
            Assert.IsTrue(operationResult.Value.Bool("waitForSync"));
            Assert.IsTrue(operationResult.Value.Long("journalSize") == journalSize);
        }
        
        [Test()]
        public async Task Should_rename_collectionAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);

            var db = new ADatabase(Database.Alias);

            var createResult = await db.Collection
                .CreateAsync(Database.TestDocumentCollectionName);

            var operationResult = await db.Collection
                .RenameAsync(createResult.Value.String("name"), Database.TestEdgeCollectionName);
            
            Assert.AreEqual(200, operationResult.StatusCode);
            Assert.IsTrue(operationResult.Success);
            Assert.IsTrue(operationResult.HasValue);
            Assert.AreEqual(createResult.Value.String("id"), operationResult.Value.String("id"));
            Assert.AreEqual(Database.TestEdgeCollectionName, operationResult.Value.String("name"));
            Assert.AreEqual(createResult.Value.Bool("isSystem"), operationResult.Value.Bool("isSystem"));
            Assert.AreEqual(createResult.Value.Int("status"), operationResult.Value.Int("status"));
            Assert.AreEqual(createResult.Value.Int("type"), operationResult.Value.Int("type"));
        }
        
        [Test()]
        public async Task Should_fail_to_rotate_collection_journalAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);

            var db = new ADatabase(Database.Alias);

            var createResult = await db.Collection
                .CreateAsync(Database.TestDocumentCollectionName);

            var operationResult = await db.Collection
                .RotateJournalAsync(createResult.Value.String("name"));
            
            Assert.AreEqual(400, operationResult.StatusCode);
            Assert.IsFalse(operationResult.Success);
            Assert.IsTrue(operationResult.HasValue);
            Assert.IsFalse(operationResult.Value);
        }
        
        #endregion
        
        #region Delete operations
        
        [Test()]
        public async Task Should_delete_collectionAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);

            var db = new ADatabase(Database.Alias);

            var createResult = await db.Collection
                .CreateAsync(Database.TestDocumentCollectionName);
            
            var deleteResult = await db.Collection
                .DeleteAsync(createResult.Value.String("name"));
            
            Assert.AreEqual(200, deleteResult.StatusCode);
            Assert.IsTrue(deleteResult.Success);
            Assert.IsTrue(deleteResult.HasValue);
            Assert.AreEqual(createResult.Value.String("id"), deleteResult.Value.String("id"));
        }
        
        #endregion
        
        public void Dispose()
        {
            Database.CleanupTestDatabasesAsync().Wait();
        }
    }
}
