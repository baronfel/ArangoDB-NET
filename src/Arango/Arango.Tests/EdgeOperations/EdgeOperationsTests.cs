using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Arango.Client;
using System.Threading.Tasks;

namespace Arango.Tests
{
    [TestFixture()]
    public class EdgeOperationsTests : IDisposable
    {
        readonly List<Dictionary<string, object>> _documents;
        
        public EdgeOperationsTests()
        {
            Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral).Wait();
			Database.CreateTestCollectionAsync(Database.TestDocumentCollectionName, ACollectionType.Document).Wait();
			Database.CreateTestCollectionAsync(Database.TestEdgeCollectionName, ACollectionType.Edge).Wait();
			
			_documents = Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName).Result;
        }
        
        #region Create operations
        
        [Test()]
        public async Task Should_create_empty_edge()
        {
        	await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID());
            
            Assert.AreEqual(202, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            Assert.IsTrue(createResult.HasValue);
            Assert.IsTrue(createResult.Value.IsString("_id"));
            Assert.IsTrue(createResult.Value.IsString("_key"));
            Assert.IsTrue(createResult.Value.IsString("_rev"));
        }
        
        [Test()]
        public async Task Should_create_empty_edge_with_waitForSync()
        {
        	await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var createResult = await db
                .Document
                .WaitForSync(true)
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID());
            
            Assert.AreEqual(201, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            Assert.IsTrue(createResult.HasValue);
            Assert.IsTrue(createResult.Value.IsString("_id"));
            Assert.IsTrue(createResult.Value.IsString("_key"));
            Assert.IsTrue(createResult.Value.IsString("_rev"));
        }
        
        [Test()]
        public async Task Should_create_edge()
        {
        	await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .From(_documents[0].ID())
                .To(_documents[1].ID())
        		.String("foo", "foo string value")
        		.Int("bar", 12345);

            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, document);
            
            Assert.AreEqual(202, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            Assert.IsTrue(createResult.HasValue);
            Assert.IsTrue(createResult.Value.IsString("_id"));
            Assert.IsTrue(createResult.Value.IsString("_key"));
            Assert.IsTrue(createResult.Value.IsString("_rev"));
            
            var getResult = await db
                .Document
                .GetAsync(createResult.Value.ID());
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(getResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), createResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), createResult.Value.Rev());
            Assert.IsTrue(getResult.Value.IsString("_from"));
            Assert.IsTrue(getResult.Value.IsString("_to"));
            Assert.AreEqual(getResult.Value.String("foo"), document.String("foo"));
            Assert.AreEqual(getResult.Value.Int("bar"), document.Int("bar"));
        }

        [Test()]
        public async Task Should_create_edge_with_returnNew_parameter()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .From(_documents[0].ID())
                .To(_documents[1].ID())
                .String("foo", "foo string value")
                .Int("bar", 12345);

            var createResult = await db
                .Document
                .ReturnNew()
                .CreateEdgeAsync(Database.TestEdgeCollectionName, document);

            Assert.AreEqual(202, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            Assert.IsTrue(createResult.HasValue);
            Assert.IsTrue(createResult.Value.IsString("_id"));
            Assert.IsTrue(createResult.Value.IsString("_key"));
            Assert.IsTrue(createResult.Value.IsString("_rev"));
            Assert.IsTrue(createResult.Value.Has("new"));
            Assert.AreEqual(createResult.Value.ID(), createResult.Value.String("new._id"));
            Assert.AreEqual(createResult.Value.Key(), createResult.Value.String("new._key"));
            Assert.AreEqual(createResult.Value.Rev(), createResult.Value.String("new._rev"));
            Assert.AreEqual(document.String("_from"), createResult.Value.String("new._from"));
            Assert.AreEqual(document.String("_to"), createResult.Value.String("new._to"));
            Assert.AreEqual(document.String("foo"), createResult.Value.String("new.foo"));
            Assert.AreEqual(document.Int("bar"), createResult.Value.Int("new.bar"));
        }

        [Test()]
        public async Task Should_create_edge_from_generic_object()
        {
        	await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var dummy = new Dummy();
            dummy.Foo = "foo string value";
            dummy.Bar = 12345;

            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), dummy);
            
            Assert.AreEqual(202, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            Assert.IsTrue(createResult.HasValue);
            Assert.IsTrue(createResult.Value.IsString("_id"));
            Assert.IsTrue(createResult.Value.IsString("_key"));
            Assert.IsTrue(createResult.Value.IsString("_rev"));
            
            var getResult = await db
                .Document
                .GetAsync<Dictionary<string, object>>(createResult.Value.ID());
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(getResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), createResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), createResult.Value.Rev());
            Assert.IsTrue(getResult.Value.IsString("_from"));
            Assert.IsTrue(getResult.Value.IsString("_to"));
            Assert.AreEqual(getResult.Value.String("foo"), dummy.Foo);
            Assert.AreEqual(getResult.Value.Int("bar"), dummy.Bar);
            Assert.AreEqual(0, dummy.Baz);
        }
        
        #endregion
        
        #region Check operations
        
        [Test()]
        public async Task Should_check_edge()
        {
        	await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var checkResult = await db
                .Document
                .CheckAsync(createResult.Value.ID());
            
            Assert.AreEqual(200, checkResult.StatusCode);
            Assert.IsTrue(checkResult.Success);
            Assert.IsTrue(checkResult.HasValue);
            Assert.AreEqual(checkResult.Value, createResult.Value.Rev());
        }
        
        [Test()]
        public async Task Should_check_edge_with_ifMatch()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var checkResult = await db
                .Document
                .IfMatch(createResult.Value.Rev())
                .CheckAsync(createResult.Value.ID());
            
            Assert.AreEqual(200, checkResult.StatusCode);
            Assert.IsTrue(checkResult.Success);
            Assert.IsTrue(checkResult.HasValue);
            Assert.AreEqual(checkResult.Value, createResult.Value.Rev());
        }
        
        [Test()]
        public async Task Should_check_edge_with_ifMatch_and_return_412()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var checkResult = await db
                .Document
                .IfMatch("123456789")
                .CheckAsync(createResult.Value.ID());
            
            Assert.AreEqual(412, checkResult.StatusCode);
            Assert.IsFalse(checkResult.Success);
            Assert.IsTrue(checkResult.HasValue);
            Assert.AreEqual(checkResult.Value, createResult.Value.Rev());
        }
        
        [Test()]
        public async Task Should_check_edge_with_ifNoneMatch()
        {
        	await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var checkResult = await db
                .Document
                .IfNoneMatch("123456789")
                .CheckAsync(createResult.Value.ID());
            
            Assert.AreEqual(200, checkResult.StatusCode);
            Assert.IsTrue(checkResult.Success);
            Assert.IsTrue(checkResult.HasValue);
            Assert.AreEqual(checkResult.Value, createResult.Value.Rev());
        }
        
        [Test()]
        public async Task Should_check_edge_with_ifNoneMatch_and_return_304()
        {
        	await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var checkResult = await db
                .Document
                .IfNoneMatch(createResult.Value.Rev())
                .CheckAsync(createResult.Value.ID());
            
            Assert.AreEqual(304, checkResult.StatusCode);
            Assert.IsFalse(checkResult.Success);
            Assert.IsTrue(checkResult.HasValue);
            Assert.AreEqual(checkResult.Value, createResult.Value.Rev());
        }
        
        #endregion
        
        #region Get operations
        
        [Test()]
        public async Task Should_get_edge()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
        	var db = new ADatabase(Database.Alias);
        	
            var document = new Dictionary<string, object>()
        		.String("foo", "foo string value")
        		.Int("bar", 12345);

            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var getResult = await db
                .Document
                .GetAsync(createResult.Value.ID());
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(getResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), createResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), createResult.Value.Rev());
            Assert.IsTrue(getResult.Value.IsString("_from"));
            Assert.IsTrue(getResult.Value.IsString("_to"));
            Assert.IsTrue(getResult.Value.IsString("foo"));
            // integers are by default parsed as long type
            Assert.IsTrue(getResult.Value.IsLong("bar"));
        }
        
        [Test()]
        public async Task Should_get_edge_with_ifMatch()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
        	var db = new ADatabase(Database.Alias);
        	
            var document = new Dictionary<string, object>()
        		.String("foo", "foo string value")
        		.Int("bar", 12345);

            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var getResult = await db
                .Document
                .IfMatch(createResult.Value.Rev())
                .GetAsync<Dictionary<string, object>>(createResult.Value.ID());
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(getResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), createResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), createResult.Value.Rev());
            Assert.IsTrue(getResult.Value.IsString("_from"));
            Assert.IsTrue(getResult.Value.IsString("_to"));
            Assert.IsTrue(getResult.Value.IsString("foo"));
            // integers are by default parsed as long type
            Assert.IsTrue(getResult.Value.IsLong("bar"));
        }
        
        [Test()]
        public async Task Should_get_edge_with_ifMatch_and_return_412()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
        	var db = new ADatabase(Database.Alias);
        	
            var document = new Dictionary<string, object>()
        		.String("foo", "foo string value")
        		.Int("bar", 12345);

            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var getResult = await db
                .Document
                .IfMatch("123456789")
                .GetAsync<Dictionary<string, object>>(createResult.Value.ID());
            
            Assert.AreEqual(412, getResult.StatusCode);
            Assert.IsFalse(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(getResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), createResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), createResult.Value.Rev());
        }
        
        [Test()]
        public async Task Should_get_edge_with_ifNoneMatch()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);
        	
            var document = new Dictionary<string, object>()
        		.String("foo", "foo string value")
        		.Int("bar", 12345);

            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var getResult = await db
                .Document
                .IfNoneMatch("123456789")
                .GetAsync<Dictionary<string, object>>(createResult.Value.ID());
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(getResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), createResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), createResult.Value.Rev());
            Assert.IsTrue(getResult.Value.IsString("_from"));
            Assert.IsTrue(getResult.Value.IsString("_to"));
            Assert.IsTrue(getResult.Value.IsString("foo"));
            // integers are by default parsed as long type
            Assert.IsTrue(getResult.Value.IsLong("bar"));
        }
        
        [Test()]
        public async Task Should_get_edge_with_ifNoneMatch_and_return_304()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
        	var db = new ADatabase(Database.Alias);
        	
            var document = new Dictionary<string, object>()
        		.String("foo", "foo string value")
        		.Int("bar", 12345);

            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var getResult = await db
                .Document
                .IfNoneMatch(createResult.Value.Rev())
                .GetAsync<Dictionary<string, object>>(createResult.Value.ID());
            
            Assert.AreEqual(304, getResult.StatusCode);
            Assert.IsFalse(getResult.Success);
            Assert.IsFalse(getResult.HasValue);
        }
        
        [Test()]
        public async Task Should_get_edge_as_generic_object()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
        	var db = new ADatabase(Database.Alias);
        	
            var document = new Dictionary<string, object>()
        		.String("foo", "foo string value")
        		.Int("bar", 12345);

            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var getResult = await db
                .Document
                .GetAsync<Dummy>(createResult.Value.ID());
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(document.String("foo"), getResult.Value.Foo);
            Assert.AreEqual(document.Int("bar"), getResult.Value.Bar);
            Assert.AreEqual(0, getResult.Value.Baz);
        }
        
        #endregion
        
        #region Get in/out/any
        
        [Test()]
        public async Task Should_get_edges_in()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
        	var db = new ADatabase(Database.Alias);
        	
            var document = new Dictionary<string, object>()
        		.String("foo", "foo string value")
        		.Int("bar", 12345);

            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[1].ID(), _documents[0].ID(), document);
            
            var getResult = await db
                .Document
                .GetEdgesAsync(Database.TestEdgeCollectionName, _documents[0].ID(), ADirection.In);
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(getResult.Value.Count, 1);
            
            Assert.AreEqual(getResult.Value[0].ID(), createResult.Value.ID());
            Assert.AreEqual(getResult.Value[0].Key(), createResult.Value.Key());
            Assert.AreEqual(getResult.Value[0].Rev(), createResult.Value.Rev());
        }
        
        [Test()]
        public async Task Should_get_edges_out()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
        	var db = new ADatabase(Database.Alias);
        	
            var document = new Dictionary<string, object>()
        		.String("foo", "foo string value")
        		.Int("bar", 12345);

            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var getResult = await db
                .Document
                .GetEdgesAsync(Database.TestEdgeCollectionName, _documents[0].ID(), ADirection.Out);
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(getResult.Value.Count, 1);
            
            Assert.AreEqual(getResult.Value[0].ID(), createResult.Value.ID());
            Assert.AreEqual(getResult.Value[0].Key(), createResult.Value.Key());
            Assert.AreEqual(getResult.Value[0].Rev(), createResult.Value.Rev());
        }
        
        [Test()]
        public async Task Should_get_edges_any()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
        	var db = new ADatabase(Database.Alias);
        	
            var document = new Dictionary<string, object>()
        		.String("foo", "foo string value")
        		.Int("bar", 12345);

            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var getResult = await db
                .Document
                .GetEdgesAsync(Database.TestEdgeCollectionName, _documents[0].ID(), ADirection.Any);
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(getResult.Value.Count, 1);
            
            Assert.AreEqual(getResult.Value[0].ID(), createResult.Value.ID());
            Assert.AreEqual(getResult.Value[0].Key(), createResult.Value.Key());
            Assert.AreEqual(getResult.Value[0].Rev(), createResult.Value.Rev());
        }
        
        #endregion
        
        #region Update operations
        
        [Test()]
        public async Task Should_update_edge()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var newDocument = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Int("bar", 54321)
                .Int("baz", 12345);
            
            var updateResult = await db
                .Document
                .UpdateAsync(createResult.Value.ID(), newDocument);
            
            Assert.AreEqual(202, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(updateResult.Value.Key(), createResult.Value.Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), createResult.Value.Rev());
            
            var getResult = await db
                .Document
                .GetAsync<Dictionary<string, object>>(updateResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), updateResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), updateResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), updateResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), document.String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), newDocument.String("foo"));
            
            Assert.AreNotEqual(getResult.Value.Int("bar"), document.Int("bar"));
            Assert.AreEqual(getResult.Value.Int("bar"), newDocument.Int("bar"));
            
            // by default JSON integers are deserialized to long type
            Assert.IsTrue(getResult.Value.IsLong("baz"));
        }

        [Test()]
        public async Task Should_update_edge_with_returnOld()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);

            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);

            var newDocument = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Int("bar", 54321)
                .Int("baz", 12345);

            var updateResult = await db
                .Document
                .ReturnOld()
                .UpdateAsync(createResult.Value.ID(), newDocument);

            Assert.AreEqual(202, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(updateResult.Value.Key(), createResult.Value.Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), createResult.Value.Rev());
            Assert.IsTrue(updateResult.Value.Has("old"));
        }

        [Test()]
        public async Task Should_update_edge_with_returnNew()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);

            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);

            var newDocument = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Int("bar", 54321)
                .Int("baz", 12345);

            var updateResult = await db
                .Document
                .ReturnNew()
                .UpdateAsync(createResult.Value.ID(), newDocument);

            Assert.AreEqual(202, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(updateResult.Value.Key(), createResult.Value.Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), createResult.Value.Rev());
            Assert.IsTrue(updateResult.Value.Has("new"));
        }

        [Test()]
        public async Task Should_update_edge_with_waitForSync()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var newDocument = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Int("bar", 54321)
                .Int("baz", 12345);
            
            var updateResult = await db
                .Document
                .WaitForSync(true)
                .UpdateAsync(createResult.Value.ID(), newDocument);
            
            Assert.AreEqual(201, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(updateResult.Value.Key(), createResult.Value.Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), createResult.Value.Rev());
            
            var getResult = await db
                .Document
                .GetAsync<Dictionary<string, object>>(updateResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), updateResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), updateResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), updateResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), document.String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), newDocument.String("foo"));
            
            Assert.AreNotEqual(getResult.Value.Int("bar"), document.Int("bar"));
            Assert.AreEqual(getResult.Value.Int("bar"), newDocument.Int("bar"));
            
            // by default JSON integers are deserialized to long type
            Assert.IsTrue(getResult.Value.IsLong("baz"));
        }

        [Test()]
        public async Task Should_update_edge_with_ignoreRevs_set_to_false()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);

            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);

            var newDocument = new Dictionary<string, object>()
                .Rev(createResult.Value.Rev())
                .String("foo", "some other new string")
                .Int("bar", 54321)
                .Int("baz", 12345);

            var updateResult = await db
                .Document
                .IgnoreRevs(false)
                .UpdateAsync(createResult.Value.ID(), newDocument);

            Assert.AreEqual(202, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(updateResult.Value.Key(), createResult.Value.Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), createResult.Value.Rev());
        }

        [Test()]
        public async Task Should_update_edge_with_ifMatch()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var newDocument = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Int("bar", 54321)
                .Int("baz", 12345);
            
            var updateResult = await db
                .Document
                .IfMatch(createResult.Value.Rev())
                .UpdateAsync(createResult.Value.ID(), newDocument);
            
            Assert.AreEqual(202, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(updateResult.Value.Key(), createResult.Value.Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), createResult.Value.Rev());
            
            var getResult = await db
                .Document
                .GetAsync<Dictionary<string, object>>(updateResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), updateResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), updateResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), updateResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), document.String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), newDocument.String("foo"));
            
            Assert.AreNotEqual(getResult.Value.Int("bar"), document.Int("bar"));
            Assert.AreEqual(getResult.Value.Int("bar"), newDocument.Int("bar"));
            
            // by default JSON integers are deserialized to long type
            Assert.IsTrue(getResult.Value.IsLong("baz"));
        }
        
        [Test()]
        public async Task Should_update_edge_with_ifMatch_and_return_412()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var newDocument = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Int("bar", 54321)
                .Int("baz", 12345);
            
            var updateResult = await db
                .Document
                .IfMatch("123456789")
                .UpdateAsync(createResult.Value.ID(), newDocument);
            
            Assert.AreEqual(412, updateResult.StatusCode);
            Assert.IsFalse(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(updateResult.Value.Key(), createResult.Value.Key());
            Assert.AreEqual(updateResult.Value.Rev(), createResult.Value.Rev());
        }
        
        [Test()]
        public async Task Should_update_edge_with_keepNull()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Object("bar", null);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            document.Merge(createResult.Value);
            
            var newDocument = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Object("baz", null);
            
            var updateResult = await db
                .Document
                .KeepNull(false)
                .UpdateAsync(createResult.Value.ID(), newDocument);
            
            Assert.AreEqual(202, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), document.ID());
            Assert.AreEqual(updateResult.Value.Key(), document.Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), document.Rev());
            
            var getResult = await db
                .Document
                .GetAsync<Dictionary<string, object>>(updateResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), updateResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), updateResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), updateResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), document.String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), newDocument.String("foo"));
            
            Assert.IsTrue(getResult.Value.Has("bar"));
            
            Assert.IsFalse(getResult.Value.Has("baz"));
        }
        
        [Test()]
        public async Task Should_update_edge_with_mergeArrays_set_to_true()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Document("bar", new Dictionary<string, object>().String("foo", "string value"));
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            document.Merge(createResult.Value);
            
            var newDocument = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Document("bar", new Dictionary<string, object>().String("bar", "other string value"));
            
            var updateResult = await db
                .Document
                .MergeObjects(true) // this is also default behavior
                .UpdateAsync(createResult.Value.ID(), newDocument);
            
            Assert.AreEqual(202, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), document.ID());
            Assert.AreEqual(updateResult.Value.Key(), document.Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), document.Rev());
            
            var getResult = await db
                .Document
                .GetAsync<Dictionary<string, object>>(updateResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), updateResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), updateResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), updateResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), document.String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), newDocument.String("foo"));
            
            Assert.IsTrue(getResult.Value.Has("bar.foo"));
            
            Assert.IsTrue(getResult.Value.Has("bar.bar"));
        }
        
        [Test()]
        public async Task Should_update_edge_with_mergeArrays_set_to_false()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Document("bar", new Dictionary<string, object>().String("foo", "string value"));
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            document.Merge(createResult.Value);
            
            var newDocument = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Document("bar", new Dictionary<string, object>().String("bar", "other string value"));
            
            var updateResult = await db
                .Document
                .MergeObjects(false)
                .UpdateAsync(createResult.Value.ID(), newDocument);
            
            Assert.AreEqual(202, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), document.ID());
            Assert.AreEqual(updateResult.Value.Key(), document.Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), document.Rev());
            
            var getResult = await db
                .Document
                .GetAsync<Dictionary<string, object>>(updateResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), updateResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), updateResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), updateResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), document.String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), newDocument.String("foo"));
            
            Assert.IsFalse(getResult.Value.Has("bar.foo"));
            
            Assert.IsTrue(getResult.Value.Has("bar.bar"));
        }
        
        [Test()]
        public async Task Should_update_edge_with_generic_object()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var dummy = new Dummy();
            dummy.Foo = "some other new string";
            dummy.Bar = 54321;
            dummy.Baz = 12345;
            
            var updateResult = await db
                .Document
                .UpdateAsync(createResult.Value.ID(), dummy);
            
            Assert.AreEqual(202, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(updateResult.Value.Key(), createResult.Value.Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), createResult.Value.Rev());
            
            var getResult = await db
                .Document
                .GetAsync<Dictionary<string, object>>(updateResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), updateResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), updateResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), updateResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), document.String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), dummy.Foo);
            
            Assert.AreNotEqual(getResult.Value.Int("bar"), document.Int("bar"));
            Assert.AreEqual(getResult.Value.Int("bar"), dummy.Bar);
            Assert.AreEqual(getResult.Value.Int("baz"), dummy.Baz);
        }
        
        #endregion
        
        #region Replace operations
        
        [Test()]
        public async Task Should_replace_edge()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var newDocument = new Dictionary<string, object>()
                .From(_documents[0].ID())
                .To(_documents[1].ID())
                .String("foo", "some other new string")
                .Int("baz", 54321);
            
            var replaceResult = await db
                .Document
                .ReplaceAsync(createResult.Value.ID(), newDocument);
            
            Assert.AreEqual(202, replaceResult.StatusCode);
            Assert.IsTrue(replaceResult.Success);
            Assert.IsTrue(replaceResult.HasValue);
            Assert.AreEqual(replaceResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(replaceResult.Value.Key(), createResult.Value.Key());
            Assert.AreNotEqual(replaceResult.Value.Rev(), createResult.Value.Rev());
            
            var getResult = await db
                .Document
                .GetAsync<Dictionary<string, object>>(replaceResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), replaceResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), replaceResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), replaceResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), document.String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), newDocument.String("foo"));
            
            Assert.AreEqual(getResult.Value.Int("baz"), newDocument.Int("baz"));

            Assert.IsFalse(getResult.Value.Has("bar"));
        }

        [Test()]
        public async Task Should_replace_edge_with_returnOld()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);

            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);

            var newDocument = new Dictionary<string, object>()
                .From(_documents[0].ID())
                .To(_documents[1].ID())
                .String("foo", "some other new string")
                .Int("baz", 54321);

            var replaceResult = await db
                .Document
                .ReturnOld()
                .ReplaceAsync(createResult.Value.ID(), newDocument);

            Assert.AreEqual(202, replaceResult.StatusCode);
            Assert.IsTrue(replaceResult.Success);
            Assert.IsTrue(replaceResult.HasValue);
            Assert.AreEqual(replaceResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(replaceResult.Value.Key(), createResult.Value.Key());
            Assert.AreNotEqual(replaceResult.Value.Rev(), createResult.Value.Rev());
            Assert.IsTrue(replaceResult.Value.Has("old"));
        }

        [Test()]
        public async Task Should_replace_edge_with_returnNew()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);

            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);

            var newDocument = new Dictionary<string, object>()
                .From(_documents[0].ID())
                .To(_documents[1].ID())
                .String("foo", "some other new string")
                .Int("baz", 54321);

            var replaceResult = await db
                .Document
                .ReturnNew()
                .ReplaceAsync(createResult.Value.ID(), newDocument);

            Assert.AreEqual(202, replaceResult.StatusCode);
            Assert.IsTrue(replaceResult.Success);
            Assert.IsTrue(replaceResult.HasValue);
            Assert.AreEqual(replaceResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(replaceResult.Value.Key(), createResult.Value.Key());
            Assert.AreNotEqual(replaceResult.Value.Rev(), createResult.Value.Rev());
            Assert.IsTrue(replaceResult.Value.Has("new"));
        }

        [Test()]
        public async Task Should_replace_edge_with_waitForSync()
        {
        	await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var newDocument = new Dictionary<string, object>()
                .From(_documents[0].ID())
                .To(_documents[1].ID())
                .String("foo", "some other new string")
                .Int("baz", 54321);
            
            var replaceResult = await db
                .Document
                .WaitForSync(true)
                .ReplaceAsync(createResult.Value.ID(), newDocument);
            
            Assert.AreEqual(201, replaceResult.StatusCode);
            Assert.IsTrue(replaceResult.Success);
            Assert.IsTrue(replaceResult.HasValue);
            Assert.AreEqual(replaceResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(replaceResult.Value.Key(), createResult.Value.Key());
            Assert.AreNotEqual(replaceResult.Value.Rev(), createResult.Value.Rev());
            
            var getResult = await db
                .Document
                .GetAsync<Dictionary<string, object>>(replaceResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), replaceResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), replaceResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), replaceResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), document.String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), newDocument.String("foo"));
            
            Assert.AreEqual(getResult.Value.Int("baz"), newDocument.Int("baz"));

            Assert.IsFalse(getResult.Value.Has("bar"));
        }

        [Test()]
        public async Task Should_replace_edge_with_ignoreRevs_set_to_false()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);

            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);

            var newDocument = new Dictionary<string, object>()
                .From(_documents[0].ID())
                .To(_documents[1].ID())
                .Rev(createResult.Value.Rev())
                .String("foo", "some other new string")
                .Int("baz", 54321);

            var replaceResult = await db
                .Document
                .IgnoreRevs(false)
                .ReplaceAsync(createResult.Value.ID(), newDocument);

            Assert.AreEqual(202, replaceResult.StatusCode);
            Assert.IsTrue(replaceResult.Success);
            Assert.IsTrue(replaceResult.HasValue);
            Assert.AreEqual(replaceResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(replaceResult.Value.Key(), createResult.Value.Key());
            Assert.AreNotEqual(replaceResult.Value.Rev(), createResult.Value.Rev());
        }

        [Test()]
        public async Task Should_replace_edge_with_ifMatch()
        {
        	await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            document.Merge(createResult.Value);
            
            var newDocument = new Dictionary<string, object>()
                .From(_documents[0].ID())
                .To(_documents[1].ID())
                .String("foo", "some other new string")
                .Int("baz", 54321);
            
            var replaceResult = await db
                .Document
                .IfMatch(document.Rev())
                .ReplaceAsync(createResult.Value.ID(), newDocument);
            
            Assert.AreEqual(202, replaceResult.StatusCode);
            Assert.IsTrue(replaceResult.Success);
            Assert.IsTrue(replaceResult.HasValue);
            Assert.AreEqual(replaceResult.Value.ID(), document.ID());
            Assert.AreEqual(replaceResult.Value.Key(), document.Key());
            Assert.AreNotEqual(replaceResult.Value.Rev(), document.Rev());
            
            var getResult = await db
                .Document
                .GetAsync<Dictionary<string, object>>(replaceResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), replaceResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), replaceResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), replaceResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), document.String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), newDocument.String("foo"));
            
            Assert.AreEqual(getResult.Value.Int("baz"), newDocument.Int("baz"));
            
            Assert.IsFalse(getResult.Value.Has("bar"));
        }
        
        [Test()]
        public async Task Should_replace_edge_with_ifMatch_and_return_412()
        {
        	await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            document.Merge(createResult.Value);
            
            var newDocument = new Dictionary<string, object>()
                .From(_documents[0].ID())
                .To(_documents[1].ID())
                .String("foo", "some other new string")
                .Int("baz", 54321);
            
            var replaceResult = await db
                .Document
                .IfMatch("123456789")
                .ReplaceAsync(createResult.Value.ID(), newDocument);
            
            Assert.AreEqual(412, replaceResult.StatusCode);
            Assert.IsFalse(replaceResult.Success);
            Assert.IsTrue(replaceResult.HasValue);
            Assert.AreEqual(replaceResult.Value.ID(), document.ID());
            Assert.AreEqual(replaceResult.Value.Key(), document.Key());
            Assert.AreEqual(replaceResult.Value.Rev(), document.Rev());
        }
        
        [Test()]
        public async Task Should_replace_edge_with_generic_object()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var dummy = new Dummy();
            dummy.Foo = "some other new string";
            dummy.Baz = 54321;
            
            var replaceResult = await db
                .Document
                .ReplaceEdge(createResult.Value.ID(), _documents[0].ID(), _documents[1].ID(), dummy);
            
            Assert.AreEqual(202, replaceResult.StatusCode);
            Assert.IsTrue(replaceResult.Success);
            Assert.IsTrue(replaceResult.HasValue);
            Assert.AreEqual(replaceResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(replaceResult.Value.Key(), createResult.Value.Key());
            Assert.AreNotEqual(replaceResult.Value.Rev(), createResult.Value.Rev());
            
            var getResult = await db
                .Document
                .GetAsync<Dictionary<string, object>>(replaceResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), replaceResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), replaceResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), replaceResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), document.String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), dummy.Foo);
            
            Assert.AreEqual(getResult.Value.Int("baz"), dummy.Baz);

            Assert.AreEqual(0, getResult.Value.Int("bar"));
        }
        
        #endregion
        
        #region Delete operations
        
        [Test()]
        public async Task Should_delete_edge()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var deleteResult = await db.Document
                .DeleteAsync(createResult.Value.ID());
            
            Assert.AreEqual(202, deleteResult.StatusCode);
            Assert.IsTrue(deleteResult.Success);
            Assert.IsTrue(deleteResult.HasValue);
            Assert.AreEqual(deleteResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(deleteResult.Value.Key(), createResult.Value.Key());
            Assert.AreEqual(deleteResult.Value.Rev(), createResult.Value.Rev());
        }
        
        [Test()]
        public async Task Should_delete_edge_with_waitForSync()
        {
        	await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var deleteResult = await db.Document
                .WaitForSync(true)
                .DeleteAsync(createResult.Value.ID());
            
            Assert.AreEqual(200, deleteResult.StatusCode);
            Assert.IsTrue(deleteResult.Success);
            Assert.IsTrue(deleteResult.HasValue);
            Assert.AreEqual(deleteResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(deleteResult.Value.Key(), createResult.Value.Key());
            Assert.AreEqual(deleteResult.Value.Rev(), createResult.Value.Rev());
        }
        
        [Test()]
        public async Task Should_delete_edge_with_ifMatch()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var deleteResult = await db.Document
                .IfMatch(createResult.Value.Rev())
                .DeleteAsync(createResult.Value.ID());
            
            Assert.AreEqual(202, deleteResult.StatusCode);
            Assert.IsTrue(deleteResult.Success);
            Assert.IsTrue(deleteResult.HasValue);
            Assert.AreEqual(deleteResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(deleteResult.Value.Key(), createResult.Value.Key());
            Assert.AreEqual(deleteResult.Value.Rev(), createResult.Value.Rev());
        }
        
        [Test()]
        public async Task Should_delete_edge_with_ifMatch_and_return_412()
        {
            await Database.ClearTestCollectionAsync(Database.TestEdgeCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);
            
            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);
            
            var deleteResult = await db.Document
                .IfMatch("123456789")
                .DeleteAsync(createResult.Value.ID());
            
            Assert.AreEqual(412, deleteResult.StatusCode);
            Assert.IsFalse(deleteResult.Success);
            Assert.IsTrue(deleteResult.HasValue);
            Assert.AreEqual(deleteResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(deleteResult.Value.Key(), createResult.Value.Key());
            Assert.AreEqual(deleteResult.Value.Rev(), createResult.Value.Rev());
        }

        [Test()]
        public async Task Should_delete_edge_with_returnOld_parameter()
        {
            var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some string")
                .Int("bar", 12345);

            var createResult = await db
                .Document
                .CreateEdgeAsync(Database.TestEdgeCollectionName, _documents[0].ID(), _documents[1].ID(), document);

            var deleteResult = await db.Document
                .ReturnOld()
                .DeleteAsync(createResult.Value.ID());

            Assert.AreEqual(202, deleteResult.StatusCode);
            Assert.IsTrue(deleteResult.Success);
            Assert.AreEqual(deleteResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(deleteResult.Value.Key(), createResult.Value.Key());
            Assert.AreEqual(deleteResult.Value.Rev(), createResult.Value.Rev());
            Assert.IsTrue(deleteResult.Value.Has("old"));
        }

        #endregion

        public void Dispose()
        {
            Database.DeleteTestDatabaseAsync(Database.TestDatabaseGeneral).Wait();
        }
    }
}
