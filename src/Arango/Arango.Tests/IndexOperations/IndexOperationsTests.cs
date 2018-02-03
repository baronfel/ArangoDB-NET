using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Arango.Client;
using System.Threading.Tasks;

namespace Arango.Tests
{
    [TestFixture()]
    public class IndexOperationsTests : IDisposable
    {
        public IndexOperationsTests()
		{
			Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral).Wait();
		}
        
        [Test()]
        public async Task Should_create_fulltext_indexAsync()
        {
        	await Database.CreateTestCollectionAsync(Database.TestDocumentCollectionName, ACollectionType.Document);
            var db = new ADatabase(Database.Alias);

            var createResult = await db.Index
                .Type(AIndexType.Fulltext)
                .Fields("foo")
                .CreateAsync(Database.TestDocumentCollectionName);
            
            Assert.AreEqual(201, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            Assert.IsTrue(createResult.HasValue);
            Assert.IsTrue(createResult.Value.IsID("id"));
            Assert.AreEqual(AIndexType.Fulltext, createResult.Value.Enum<AIndexType>("type"));
            Assert.IsFalse(createResult.Value.Bool("unique"));
            Assert.IsTrue(createResult.Value.Bool("sparse"));
            
            Assert.AreEqual(1, createResult.Value.Size("fields"));
            new List<string> { "foo" }.ForEach(field => Assert.IsTrue(createResult.Value.List<string>("fields").Contains(field)));
            
            Assert.IsTrue(createResult.Value.Bool("isNewlyCreated"));
        }
        
        [Test()]
        public async Task Should_create_geo_indexAsync()
        {
        	await Database.CreateTestCollectionAsync(Database.TestDocumentCollectionName, ACollectionType.Document);
            var db = new ADatabase(Database.Alias);

            var createResult = await db.Index
                .Type(AIndexType.Geo)
                .Fields("foo")
                .CreateAsync(Database.TestDocumentCollectionName);
            
            Assert.AreEqual(201, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            Assert.IsTrue(createResult.HasValue);
            Assert.IsTrue(createResult.Value.IsID("id"));
            Assert.AreEqual("geo1", createResult.Value.String("type"));
            Assert.IsFalse(createResult.Value.Bool("unique"));
            Assert.IsTrue(createResult.Value.Bool("sparse"));
            
            Assert.AreEqual(1, createResult.Value.Size("fields"));
            new List<string> { "foo" }.ForEach(field => Assert.IsTrue(createResult.Value.List<string>("fields").Contains(field)));
            
            Assert.IsTrue(createResult.Value.Bool("isNewlyCreated"));
        }
        
        [Test()]
        public async Task Should_create_hash_index()
        {
        	await Database.CreateTestCollectionAsync(Database.TestDocumentCollectionName, ACollectionType.Document);
            var db = new ADatabase(Database.Alias);

            var createResult = await db.Index
                .Type(AIndexType.Hash)
                .Unique(true)
                .Fields("foo", "bar")
                .CreateAsync(Database.TestDocumentCollectionName);
            
            Assert.AreEqual(201, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            Assert.IsTrue(createResult.HasValue);
            Assert.IsTrue(createResult.Value.IsID("id"));
            Assert.AreEqual(AIndexType.Hash, createResult.Value.Enum<AIndexType>("type"));
            Assert.IsTrue(createResult.Value.Bool("unique"));
            Assert.IsFalse(createResult.Value.Bool("sparse"));
            Assert.AreEqual(1, createResult.Value.Int("selectivityEstimate"));
            
            Assert.AreEqual(2, createResult.Value.Size("fields"));
            new List<string> { "foo", "bar" }.ForEach(field => Assert.IsTrue(createResult.Value.List<string>("fields").Contains(field)));
            
            Assert.IsTrue(createResult.Value.Bool("isNewlyCreated"));
        }
        
        [Test()]
        public async Task Should_create_skiplist_index()
        {
        	await Database.CreateTestCollectionAsync(Database.TestDocumentCollectionName, ACollectionType.Document);
            var db = new ADatabase(Database.Alias);

            var createResult = await db.Index
                .Type(AIndexType.Skiplist)
                .Unique(false)
                .Fields("foo", "bar")
                .CreateAsync(Database.TestDocumentCollectionName);
            
            Assert.AreEqual(201, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            Assert.IsTrue(createResult.HasValue);
            Assert.IsTrue(createResult.Value.IsID("id"));
            Assert.AreEqual(AIndexType.Skiplist, createResult.Value.Enum<AIndexType>("type"));
            Assert.IsFalse(createResult.Value.Bool("unique"));
            Assert.IsFalse(createResult.Value.Bool("sparse"));
            
            Assert.AreEqual(2, createResult.Value.Size("fields"));
            new List<string> { "foo", "bar" }.ForEach(field => Assert.IsTrue(createResult.Value.List<string>("fields").Contains(field)));
            
            Assert.IsTrue(createResult.Value.Bool("isNewlyCreated"));
        }
        
        [Test()]
        public async Task Should_recreate_hash_index()
        {
        	await Database.CreateTestCollectionAsync(Database.TestDocumentCollectionName, ACollectionType.Document);
            var db = new ADatabase(Database.Alias);

            var createResult = await db.Index
                .Type(AIndexType.Hash)
                .Unique(true)
                .Fields("foo", "bar")
                .CreateAsync(Database.TestDocumentCollectionName);
            
            Assert.AreEqual(201, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            Assert.IsTrue(createResult.HasValue);
            Assert.IsTrue(createResult.Value.IsID("id"));
            Assert.AreEqual(AIndexType.Hash, createResult.Value.Enum<AIndexType>("type"));
            Assert.IsTrue(createResult.Value.Bool("unique"));
            Assert.IsFalse(createResult.Value.Bool("sparse"));
            Assert.AreEqual(1, createResult.Value.Int("selectivityEstimate"));
            
            Assert.AreEqual(2, createResult.Value.Size("fields"));
            new List<string> { "foo", "bar" }.ForEach(field => Assert.IsTrue(createResult.Value.List<string>("fields").Contains(field)));
            
            Assert.IsTrue(createResult.Value.Bool("isNewlyCreated"));
            
            var recreateResult = await db.Index
                .Type(AIndexType.Hash)
                .Unique(true)
                .Fields("foo", "bar")
                .CreateAsync(Database.TestDocumentCollectionName);
            
            Assert.AreEqual(200, recreateResult.StatusCode);
            Assert.IsTrue(recreateResult.Success);
            Assert.IsTrue(recreateResult.HasValue);
            Assert.IsTrue(recreateResult.Value.IsID("id"));
            Assert.AreEqual(AIndexType.Hash, recreateResult.Value.Enum<AIndexType>("type"));
            Assert.IsTrue(recreateResult.Value.Bool("unique"));
            Assert.IsFalse(recreateResult.Value.Bool("sparse"));
            Assert.AreEqual(1, recreateResult.Value.Int("selectivityEstimate"));
            
            Assert.AreEqual(2, recreateResult.Value.Size("fields"));
            new List<string> { "foo", "bar" }.ForEach(field => Assert.IsTrue(recreateResult.Value.List<string>("fields").Contains(field)));
            
            Assert.IsFalse(recreateResult.Value.Bool("isNewlyCreated"));
        }
        
        [Test()]
        public async Task Should_get_index()
        {
        	await Database.CreateTestCollectionAsync(Database.TestDocumentCollectionName, ACollectionType.Document);
            var db = new ADatabase(Database.Alias);

            var createResult = await db.Index
                .Type(AIndexType.Hash)
                .Unique(true)
                .Fields("foo", "bar")
                .CreateAsync(Database.TestDocumentCollectionName);
            
            Assert.AreEqual(201, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            
            var getResult = await db.Index
                .GetAsync(createResult.Value.String("id"));
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.IsTrue(getResult.Value.IsID("id"));
            Assert.AreEqual(createResult.Value.String("id"), getResult.Value.String("id"));
            Assert.AreEqual(AIndexType.Hash, getResult.Value.Enum<AIndexType>("type"));
            Assert.IsTrue(getResult.Value.Bool("unique"));
            Assert.IsFalse(getResult.Value.Bool("sparse"));
            Assert.AreEqual(1, getResult.Value.Int("selectivityEstimate"));
            
            Assert.AreEqual(2, getResult.Value.Size("fields"));
            new List<string> { "foo", "bar" }.ForEach(field => Assert.IsTrue(getResult.Value.List<string>("fields").Contains(field)));
        }
        
        [Test()]
        public async Task Should_delete_index()
        {
        	await Database.CreateTestCollectionAsync(Database.TestDocumentCollectionName, ACollectionType.Document);
            var db = new ADatabase(Database.Alias);

            var createResult = await db.Index
                .Type(AIndexType.Hash)
                .Unique(true)
                .Fields("foo", "bar")
                .CreateAsync(Database.TestDocumentCollectionName);
            
            Assert.AreEqual(201, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            
            var deleteResult = await db.Index
                .DeleteAsync(createResult.Value.String("id"));
            
            Assert.AreEqual(200, deleteResult.StatusCode);
            Assert.IsTrue(deleteResult.Success);
            Assert.IsTrue(deleteResult.HasValue);
            Assert.IsTrue(deleteResult.Value.IsID("id"));
            Assert.AreEqual(createResult.Value.String("id"), deleteResult.Value.String("id"));
        }
        
        public void Dispose()
        {
            Database.CleanupTestDatabasesAsync().Wait();
        }
    }
}
