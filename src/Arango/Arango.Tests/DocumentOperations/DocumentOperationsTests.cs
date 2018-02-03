using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Arango.Client;
using System.Threading.Tasks;

namespace Arango.Tests
{
    [TestFixture()]
    public class DocumentOperationsTests : IDisposable
    {
        public DocumentOperationsTests()
		{
			Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral).Wait();
			Database.CreateTestCollectionAsync(Database.TestDocumentCollectionName, ACollectionType.Document).Wait();
		}
        
        #region Create operations
        
        [Test()]
        public async Task Should_create_document()
        {
        	await Database.ClearTestCollectionAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
        		.String("foo", "foo string value")
        		.Int("bar", 12345);

            var createResult = await db.Document
                .CreateAsync(Database.TestDocumentCollectionName, document);
            
            Assert.AreEqual(202, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            Assert.IsTrue(createResult.HasValue);
            Assert.IsTrue(createResult.Value.IsString("_id"));
            Assert.IsTrue(createResult.Value.IsString("_key"));
            Assert.IsTrue(createResult.Value.IsString("_rev"));
        }

        [Test()]
        public async Task Should_create_document_with_returnNew_parameter()
        {
            await Database.ClearTestCollectionAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "foo string value")
                .Int("bar", 12345);

            var createResult = await db.Document
                .ReturnNew()
                .CreateAsync(Database.TestDocumentCollectionName, document);

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
            Assert.AreEqual(document.String("foo"), createResult.Value.String("new.foo"));
            Assert.AreEqual(document.Int("bar"), createResult.Value.Int("new.bar"));
        }

        [Test()]
        public async Task Should_create_document_with_waitForSync()
        {
        	await Database.ClearTestCollectionAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
        		.String("foo", "foo string value")
        		.Int("bar", 12345);

            var createResult = await db.Document
                .WaitForSync(true)
                .CreateAsync(Database.TestDocumentCollectionName, document);
            
            Assert.AreEqual(201, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            Assert.IsTrue(createResult.HasValue);
            Assert.IsTrue(createResult.Value.IsString("_id"));
            Assert.IsTrue(createResult.Value.IsString("_key"));
            Assert.IsTrue(createResult.Value.IsString("_rev"));
        }

        [Test()]
        public async Task Should_create_document_from_generic_object()
        {
        	await Database.ClearTestCollectionAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);
            
            var dummy = new Dummy();
            dummy.Foo = "foo string value";
            dummy.Bar = 12345;
            
            var createResult = await db.Document
                .CreateAsync(Database.TestDocumentCollectionName, dummy);
            
            Assert.AreEqual(202, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            Assert.IsTrue(createResult.HasValue);
            Assert.IsTrue(createResult.Value.IsString("_id"));
            Assert.IsTrue(createResult.Value.IsString("_key"));
            Assert.IsTrue(createResult.Value.IsString("_rev"));
            
            var getResult = await db.Document
                .GetAsync<Dictionary<string, object>>(createResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), createResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), createResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), createResult.Value.Rev());
            Assert.AreEqual(getResult.Value.String("foo"), dummy.Foo);
            Assert.AreEqual(getResult.Value.Int("bar"), dummy.Bar);
            Assert.AreEqual(0, dummy.Baz);
        }
        
        [Test()]
        public async Task Should_create_document_with_custom_ID()
        {
            await Database.ClearTestCollectionAsync(Database.TestDocumentCollectionName);
        	var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("_key", "1234-5678")
        		.String("foo", "foo string value")
        		.Int("bar", 12345);

            var createResult = await db.Document
                .CreateAsync(Database.TestDocumentCollectionName, document);
            
            Assert.AreEqual(202, createResult.StatusCode);
            Assert.IsTrue(createResult.Success);
            Assert.IsTrue(createResult.HasValue);
            Assert.AreEqual(Database.TestDocumentCollectionName + "/" + document.Key(), createResult.Value.ID());
            Assert.AreEqual(document.Key(), createResult.Value.Key());
            Assert.IsTrue(createResult.Value.IsString("_rev"));
        }
        
        #endregion
        
        #region Check operations
        
        [Test()]
        public async Task Should_check_document()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);
            
            var checkResult = await db.Document
                .CheckAsync(documents[0].ID());
            
            Assert.AreEqual(200, checkResult.StatusCode);
            Assert.IsTrue(checkResult.Success);
            Assert.IsTrue(checkResult.HasValue);
            Assert.AreEqual(checkResult.Value, documents[0].Rev());
        }
        
        [Test()]
        public async Task Should_check_document_with_ifMatch()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);
            
            var checkResult = await db.Document
                .IfMatch(documents[0].Rev())
                .CheckAsync(documents[0].ID());
            
            Assert.AreEqual(200, checkResult.StatusCode);
            Assert.IsTrue(checkResult.Success);
            Assert.IsTrue(checkResult.HasValue);
            Assert.AreEqual(checkResult.Value, documents[0].Rev());
        }
        
        [Test()]
        public async Task Should_check_document_with_ifMatch_and_return_412()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);
            
            var checkResult = await db.Document
                .IfMatch("123456789")
                .CheckAsync(documents[0].ID());
            
            Assert.AreEqual(412, checkResult.StatusCode);
            Assert.IsFalse(checkResult.Success);
            Assert.IsTrue(checkResult.HasValue);
            Assert.AreEqual(checkResult.Value, documents[0].Rev());
        }
        
        [Test()]
        public async Task Should_check_document_with_ifNoneMatch()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);
            
            var checkResult = await db.Document
                .IfNoneMatch("123456789")
                .CheckAsync(documents[0].ID());
            
            Assert.AreEqual(200, checkResult.StatusCode);
            Assert.IsTrue(checkResult.Success);
            Assert.IsTrue(checkResult.HasValue);
            Assert.AreEqual(checkResult.Value, documents[0].Rev());
        }
        
        [Test()]
        public async Task Should_check_document_with_ifNoneMatch_and_return_304()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);
            
            var checkResult = await db.Document
                .IfNoneMatch(documents[0].Rev())
                .CheckAsync(documents[0].ID());
            
            Assert.AreEqual(304, checkResult.StatusCode);
            Assert.IsFalse(checkResult.Success);
            Assert.IsTrue(checkResult.HasValue);
            Assert.AreEqual(checkResult.Value, documents[0].Rev());
        }
        
        #endregion
        
        #region Get operations
        
        [Test()]
        public async Task Should_get_document()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
        	var db = new ADatabase(Database.Alias);
        	
            var getResult = await db.Document
                .GetAsync<Dictionary<string, object>>(documents[0].ID());
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(getResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(getResult.Value.Key(), documents[0].Key());
            Assert.AreEqual(getResult.Value.Rev(), documents[0].Rev());
            Assert.AreEqual(getResult.Value.String("foo"), documents[0].String("foo"));
            Assert.AreEqual(getResult.Value.String("bar"), documents[0].String("bar"));
        }
        
        [Test()]
        public async Task Should_get_document_with_ifMatch()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
        	var db = new ADatabase(Database.Alias);
        	
            var getResult = await db.Document
                .IfMatch(documents[0].Rev())
                .GetAsync<Dictionary<string, object>>(documents[0].ID());
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(getResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(getResult.Value.Key(), documents[0].Key());
            Assert.AreEqual(getResult.Value.Rev(), documents[0].Rev());
            Assert.AreEqual(getResult.Value.String("foo"), documents[0].String("foo"));
            Assert.AreEqual(getResult.Value.String("bar"), documents[0].String("bar"));
        }
        
        [Test()]
        public async Task Should_get_document_with_ifMatch_and_return_412()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
        	var db = new ADatabase(Database.Alias);
        	
            var getResult = await db.Document
                .IfMatch("123456789")
                .GetAsync<Dictionary<string, object>>(documents[0].ID());
            
            Assert.AreEqual(412, getResult.StatusCode);
            Assert.IsFalse(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(getResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(getResult.Value.Key(), documents[0].Key());
            Assert.AreEqual(getResult.Value.Rev(), documents[0].Rev());
        }
        
        [Test()]
        public async Task Should_get_document_with_ifNoneMatch()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
        	var db = new ADatabase(Database.Alias);
        	
            var getResult = await db.Document
                .IfNoneMatch("123456789")
                .GetAsync<Dictionary<string, object>>(documents[0].ID());
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(getResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(getResult.Value.Key(), documents[0].Key());
            Assert.AreEqual(getResult.Value.Rev(), documents[0].Rev());
            Assert.AreEqual(getResult.Value.String("foo"), documents[0].String("foo"));
            Assert.AreEqual(getResult.Value.String("bar"), documents[0].String("bar"));
        }
        
        [Test()]
        public async Task Should_get_document_with_ifNoneMatch_and_return_304()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
        	var db = new ADatabase(Database.Alias);
        	
            var getResult = await db.Document
                .IfNoneMatch(documents[0].Rev())
                .GetAsync<Dictionary<string, object>>(documents[0].ID());
            
            Assert.AreEqual(304, getResult.StatusCode);
            Assert.IsFalse(getResult.Success);
            Assert.IsFalse(getResult.HasValue);
        }
        
        [Test()]
        public async Task Should_get_document_as_generic_object()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
        	var db = new ADatabase(Database.Alias);
        	
            var getResult = await db.Document
                .GetAsync<Dummy>(documents[0].ID());
            
            Assert.AreEqual(200, getResult.StatusCode);
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            Assert.AreEqual(documents[0].String("foo"), getResult.Value.Foo);
            Assert.AreEqual(documents[0].Int("bar"), getResult.Value.Bar);
            Assert.AreEqual(0, getResult.Value.Baz);
        }
        
        #endregion
        
        #region Update operations
        
        [Test()]
        public async Task Should_update_document()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Int("bar", 54321)
                .Int("baz", 12345);
            
            var updateResult = await db.Document
                .UpdateAsync(documents[0].ID(), document);
            
            Assert.AreEqual(202, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(updateResult.Value.Key(), documents[0].Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), documents[0].Rev());
            
            var getResult = await db.Document
                .GetAsync<Dictionary<string, object>>(updateResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), updateResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), updateResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), updateResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), documents[0].String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), document.String("foo"));
            
            Assert.AreNotEqual(getResult.Value.Int("bar"), documents[0].Int("bar"));
            Assert.AreEqual(getResult.Value.Int("bar"), document.Int("bar"));
            Assert.AreEqual(getResult.Value.Int("baz"), document.Int("baz"));
        }

        [Test()]
        public async Task Should_update_document_with_returnOld()
        {
            var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Int("bar", 54321)
                .Int("baz", 12345);

            var updateResult = await db.Document
                .ReturnOld()
                .UpdateAsync(documents[0].ID(), document);

            Assert.AreEqual(202, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(updateResult.Value.Key(), documents[0].Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), documents[0].Rev());
            Assert.IsTrue(updateResult.Value.Has("old"));
        }

        [Test()]
        public async Task Should_update_document_with_returnNew()
        {
            var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Int("bar", 54321)
                .Int("baz", 12345);

            var updateResult = await db.Document
                .ReturnNew()
                .UpdateAsync(documents[0].ID(), document);

            Assert.AreEqual(202, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(updateResult.Value.Key(), documents[0].Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), documents[0].Rev());
            Assert.IsTrue(updateResult.Value.Has("new"));
        }

        [Test()]
        public async Task Should_update_document_with_waitForSync()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Int("bar", 54321)
                .Int("baz", 12345);
            
            var updateResult = await db.Document
                .WaitForSync(true)
                .UpdateAsync(documents[0].ID(), document);
            
            Assert.AreEqual(201, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(updateResult.Value.Key(), documents[0].Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), documents[0].Rev());
            
            var getResult = await db.Document
                .GetAsync<Dictionary<string, object>>(updateResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), updateResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), updateResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), updateResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), documents[0].String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), document.String("foo"));
            
            Assert.AreNotEqual(getResult.Value.Int("bar"), documents[0].Int("bar"));
            Assert.AreEqual(getResult.Value.Int("bar"), document.Int("bar"));
            Assert.AreEqual(getResult.Value.Int("baz"), document.Int("baz"));
        }

        [Test()]
        public async Task Should_update_document_with_ignoreRevs_set_to_false()
        {
            var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .Rev(documents[0].Rev())
                .String("foo", "some other new string")
                .Int("bar", 54321)
                .Int("baz", 12345);

            var updateResult = await db.Document
                .IgnoreRevs(false)
                .UpdateAsync(documents[0].ID(), document);

            Assert.AreEqual(202, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(updateResult.Value.Key(), documents[0].Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), documents[0].Rev());
        }

        [Test()]
        public async Task Should_update_document_with_ifMatch()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Int("bar", 54321)
                .Int("baz", 12345);
            
            var updateResult = await db.Document
                .IfMatch(documents[0].Rev())
                .UpdateAsync(documents[0].ID(), document);
            
            Assert.AreEqual(202, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(updateResult.Value.Key(), documents[0].Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), documents[0].Rev());
            
            var getResult = await db.Document
                .GetAsync<Dictionary<string, object>>(updateResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), updateResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), updateResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), updateResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), documents[0].String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), document.String("foo"));
            
            Assert.AreNotEqual(getResult.Value.Int("bar"), documents[0].Int("bar"));
            Assert.AreEqual(getResult.Value.Int("bar"), document.Int("bar"));
            Assert.AreEqual(getResult.Value.Int("baz"), document.Int("baz"));
        }
        
        [Test()]
        public async Task Should_update_document_with_ifMatch_and_return_412()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Int("bar", 54321)
                .Int("baz", 12345);
            
            var updateResult = await db.Document
                .IfMatch("123456789")
                .UpdateAsync(documents[0].ID(), document);
            
            Assert.AreEqual(412, updateResult.StatusCode);
            Assert.IsFalse(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(updateResult.Value.Key(), documents[0].Key());
            Assert.AreEqual(updateResult.Value.Rev(), documents[0].Rev());
        }
        
        [Test()]
        public async Task Should_update_document_with_keepNull()
        {
            var db = new ADatabase(Database.Alias);

            var newDocument = new Dictionary<string, object>()
                .String("foo", "some string")
                .Object("bar", null);
            
            var createResult = await db.Document
                .CreateAsync(Database.TestDocumentCollectionName, newDocument);
            
            newDocument.Merge(createResult.Value);
            
            var document = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Object("baz", null);
            
            var updateResult = await db.Document
                .KeepNull(false)
                .UpdateAsync(newDocument.ID(), document);
            
            Assert.AreEqual(202, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), newDocument.ID());
            Assert.AreEqual(updateResult.Value.Key(), newDocument.Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), newDocument.Rev());
            
            var getResult = await db.Document
                .GetAsync<Dictionary<string, object>>(updateResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), updateResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), updateResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), updateResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), newDocument.String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), document.String("foo"));
            
            Assert.IsTrue(getResult.Value.Has("bar"));
            
            Assert.IsFalse(getResult.Value.Has("baz"));
        }
        
        [Test()]
        public async Task Should_update_document_with_mergeArrays_set_to_true()
        {
            var db = new ADatabase(Database.Alias);

            var newDocument = new Dictionary<string, object>()
                .String("foo", "some string")
                .Document("bar", new Dictionary<string, object>().String("foo", "string value"));
            
            var createResult = await db.Document
                .CreateAsync(Database.TestDocumentCollectionName, newDocument);
            
            newDocument.Merge(createResult.Value);
            
            var document = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Document("bar", new Dictionary<string, object>().String("bar", "other string value"));
            
            var updateResult = await db.Document
                .MergeObjects(true) // this is also default behavior
                .UpdateAsync(newDocument.ID(), document);
            
            Assert.AreEqual(202, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), newDocument.ID());
            Assert.AreEqual(updateResult.Value.Key(), newDocument.Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), newDocument.Rev());
            
            var getResult = await db.Document
                .GetAsync<Dictionary<string, object>>(updateResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), updateResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), updateResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), updateResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), newDocument.String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), document.String("foo"));
            
            Assert.IsTrue(getResult.Value.Has("bar.foo"));
            
            Assert.IsTrue(getResult.Value.Has("bar.bar"));
        }
        
        [Test()]
        public async Task Should_update_document_with_mergeArrays_set_to_false()
        {
            var db = new ADatabase(Database.Alias);

            var newDocument = new Dictionary<string, object>()
                .String("foo", "some string")
                .Document("bar", new Dictionary<string, object>().String("foo", "string value"));
            
            var createResult = await db.Document
                .CreateAsync(Database.TestDocumentCollectionName, newDocument);
            
            newDocument.Merge(createResult.Value);
            
            var document = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Document("bar", new Dictionary<string, object>().String("bar", "other string value"));
            
            var updateResult = await db.Document
                .MergeObjects(false)
                .UpdateAsync(newDocument.ID(), document);
            
            Assert.AreEqual(202, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), newDocument.ID());
            Assert.AreEqual(updateResult.Value.Key(), newDocument.Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), newDocument.Rev());
            
            var getResult = await db.Document
                .GetAsync<Dictionary<string, object>>(updateResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), updateResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), updateResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), updateResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), newDocument.String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), document.String("foo"));
            
            Assert.IsFalse(getResult.Value.Has("bar.foo"));
            
            Assert.IsTrue(getResult.Value.Has("bar.bar"));
        }
        
        [Test()]
        public async Task Should_update_document_with_generic_object()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var dummy = new Dummy();
            dummy.Foo = "some other new string";
            dummy.Bar = 54321;
            dummy.Baz = 12345;
            
            var updateResult = await db.Document
                .UpdateAsync(documents[0].ID(), dummy);
            
            Assert.AreEqual(202, updateResult.StatusCode);
            Assert.IsTrue(updateResult.Success);
            Assert.IsTrue(updateResult.HasValue);
            Assert.AreEqual(updateResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(updateResult.Value.Key(), documents[0].Key());
            Assert.AreNotEqual(updateResult.Value.Rev(), documents[0].Rev());
            
            var getResult = await db.Document
                .GetAsync<Dictionary<string, object>>(updateResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), updateResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), updateResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), updateResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), documents[0].String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), dummy.Foo);
            
            Assert.AreNotEqual(getResult.Value.Int("bar"), documents[0].Int("bar"));
            Assert.AreEqual(getResult.Value.Int("bar"), dummy.Bar);
            Assert.AreEqual(getResult.Value.Int("baz"), dummy.Baz);
        }
        
        #endregion
        
        #region Replace operations
        
        [Test()]
        public async Task Should_replace_document()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Int("baz", 54321);
            
            var replaceResult = await db.Document.ReplaceAsync(documents[0].ID(), document);
            
            Assert.AreEqual(202, replaceResult.StatusCode);
            Assert.IsTrue(replaceResult.Success);
            Assert.IsTrue(replaceResult.HasValue);
            Assert.AreEqual(replaceResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(replaceResult.Value.Key(), documents[0].Key());
            Assert.AreNotEqual(replaceResult.Value.Rev(), documents[0].Rev());
            
            var getResult = await db.Document
                .GetAsync<Dictionary<string, object>>(replaceResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), replaceResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), replaceResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), replaceResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), documents[0].String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), document.String("foo"));
            
            Assert.AreEqual(getResult.Value.Int("baz"), document.Int("baz"));
            
            Assert.IsFalse(getResult.Value.Has("bar"));
        }

        [Test()]
        public async Task Should_replace_document_with_returnOld()
        {
            var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Int("baz", 54321);

            var replaceResult = await db
                .Document
                .ReturnOld()
                .ReplaceAsync(documents[0].ID(), document);

            Assert.AreEqual(202, replaceResult.StatusCode);
            Assert.IsTrue(replaceResult.Success);
            Assert.IsTrue(replaceResult.HasValue);
            Assert.AreEqual(replaceResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(replaceResult.Value.Key(), documents[0].Key());
            Assert.AreNotEqual(replaceResult.Value.Rev(), documents[0].Rev());
            Assert.IsTrue(replaceResult.Value.Has("old"));
        }

        [Test()]
        public async Task Should_replace_document_with_returnNew()
        {
            var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Int("baz", 54321);

            var replaceResult = await db
                .Document
                .ReturnNew()
                .ReplaceAsync(documents[0].ID(), document);

            Assert.AreEqual(202, replaceResult.StatusCode);
            Assert.IsTrue(replaceResult.Success);
            Assert.IsTrue(replaceResult.HasValue);
            Assert.AreEqual(replaceResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(replaceResult.Value.Key(), documents[0].Key());
            Assert.AreNotEqual(replaceResult.Value.Rev(), documents[0].Rev());
            Assert.IsTrue(replaceResult.Value.Has("new"));
        }

        [Test()]
        public async Task Should_replace_document_with_waitForSync()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Int("baz", 54321);
            
            var replaceResult = await db.Document
                .WaitForSync(true)
                .ReplaceAsync(documents[0].ID(), document);
            
            Assert.AreEqual(201, replaceResult.StatusCode);
            Assert.IsTrue(replaceResult.Success);
            Assert.IsTrue(replaceResult.HasValue);
            Assert.AreEqual(replaceResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(replaceResult.Value.Key(), documents[0].Key());
            Assert.AreNotEqual(replaceResult.Value.Rev(), documents[0].Rev());
            
            var getResult = await db.Document
                .GetAsync<Dictionary<string, object>>(replaceResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), replaceResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), replaceResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), replaceResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), documents[0].String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), document.String("foo"));
            
            Assert.AreEqual(getResult.Value.Int("baz"), document.Int("baz"));
            
            Assert.IsFalse(getResult.Value.Has("bar"));
        }

        [Test()]
        public async Task Should_replace_document_with_ignoreRevs_set_to_false()
        {
            var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .Rev(documents[0].Rev())
                .String("foo", "some other new string")
                .Int("baz", 54321);

            var replaceResult = await db.Document
                .IgnoreRevs(false)
                .ReplaceAsync(documents[0].ID(), document);

            Assert.AreEqual(202, replaceResult.StatusCode);
            Assert.IsTrue(replaceResult.Success);
            Assert.IsTrue(replaceResult.HasValue);
            Assert.AreEqual(replaceResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(replaceResult.Value.Key(), documents[0].Key());
            Assert.AreNotEqual(replaceResult.Value.Rev(), documents[0].Rev());
        }

        [Test()]
        public async Task Should_replace_document_with_ifMatch()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Int("baz", 54321);
            
            var replaceResult = await db.Document
                .IfMatch(documents[0].Rev())
                .ReplaceAsync(documents[0].ID(), document);
            
            Assert.AreEqual(202, replaceResult.StatusCode);
            Assert.IsTrue(replaceResult.Success);
            Assert.IsTrue(replaceResult.HasValue);
            Assert.AreEqual(replaceResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(replaceResult.Value.Key(), documents[0].Key());
            Assert.AreNotEqual(replaceResult.Value.Rev(), documents[0].Rev());
            
            var getResult = await db.Document
                .GetAsync<Dictionary<string, object>>(replaceResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), replaceResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), replaceResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), replaceResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), documents[0].String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), document.String("foo"));
            
            Assert.AreEqual(getResult.Value.Int("baz"), document.Int("baz"));
            
            Assert.IsFalse(getResult.Value.Has("bar"));
        }
        
        [Test()]
        public async Task Should_replace_document_with_ifMatch_and_return_412()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var document = new Dictionary<string, object>()
                .String("foo", "some other new string")
                .Int("baz", 54321);
            
            var replaceResult = await db.Document
                .IfMatch("123456789")
                .ReplaceAsync(documents[0].ID(), document);
            
            Assert.AreEqual(412, replaceResult.StatusCode);
            Assert.IsFalse(replaceResult.Success);
            Assert.IsTrue(replaceResult.HasValue);
            Assert.AreEqual(replaceResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(replaceResult.Value.Key(), documents[0].Key());
            Assert.AreEqual(replaceResult.Value.Rev(), documents[0].Rev());
        }
        
        [Test()]
        public async Task Should_replace_document_with_generic_object()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var dummy = new Dummy();
            dummy.Foo = "some other new string";
            dummy.Baz = 54321;
            
            var replaceResult = await db.Document
                .ReplaceAsync(documents[0].ID(), dummy);
            
            Assert.AreEqual(202, replaceResult.StatusCode);
            Assert.IsTrue(replaceResult.Success);
            Assert.IsTrue(replaceResult.HasValue);
            Assert.AreEqual(replaceResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(replaceResult.Value.Key(), documents[0].Key());
            Assert.AreNotEqual(replaceResult.Value.Rev(), documents[0].Rev());
            
            var getResult = await db.Document
                .GetAsync<Dictionary<string, object>>(replaceResult.Value.ID());
            
            Assert.AreEqual(getResult.Value.ID(), replaceResult.Value.ID());
            Assert.AreEqual(getResult.Value.Key(), replaceResult.Value.Key());
            Assert.AreEqual(getResult.Value.Rev(), replaceResult.Value.Rev());
            
            Assert.AreNotEqual(getResult.Value.String("foo"), documents[0].String("foo"));
            Assert.AreEqual(getResult.Value.String("foo"), dummy.Foo);
            
            Assert.AreEqual(getResult.Value.Int("baz"), dummy.Baz);
            
            Assert.AreEqual(0, dummy.Bar);
        }
        
        #endregion
        
        #region Delete operations
        
        [Test()]
        public async Task Should_delete_document()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);
            
            var deleteResult = await db.Document
                .DeleteAsync(documents[0].ID());
            
            Assert.AreEqual(202, deleteResult.StatusCode);
            Assert.IsTrue(deleteResult.Success);
            Assert.IsTrue(deleteResult.HasValue);
            Assert.AreEqual(deleteResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(deleteResult.Value.Key(), documents[0].Key());
            Assert.AreEqual(deleteResult.Value.Rev(), documents[0].Rev());
        }
        
        [Test()]
        public async Task Should_delete_document_with_waitForSync()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);
            
            var deleteResult = await db.Document
                .WaitForSync(true)
                .DeleteAsync(documents[0].ID());
            
            Assert.AreEqual(200, deleteResult.StatusCode);
            Assert.IsTrue(deleteResult.Success);
            Assert.IsTrue(deleteResult.HasValue);
            Assert.AreEqual(deleteResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(deleteResult.Value.Key(), documents[0].Key());
            Assert.AreEqual(deleteResult.Value.Rev(), documents[0].Rev());
        }
        
        [Test()]
        public async Task Should_delete_document_with_ifMatch()
        {
            var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);
            
            var deleteResult = await db.Document
                .IfMatch(documents[0].Rev())
                .DeleteAsync(documents[0].ID());
            
            Assert.AreEqual(202, deleteResult.StatusCode);
            Assert.IsTrue(deleteResult.Success);
            Assert.IsTrue(deleteResult.HasValue);
            Assert.AreEqual(deleteResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(deleteResult.Value.Key(), documents[0].Key());
            Assert.AreEqual(deleteResult.Value.Rev(), documents[0].Rev());
        }
        
        [Test()]
        public async Task Should_delete_document_with_ifMatch_and_return_412()
        {
        	var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var deleteResult = await db.Document
                .IfMatch("123456789")
                .DeleteAsync(documents[0].ID());
            
            Assert.AreEqual(412, deleteResult.StatusCode);
            Assert.IsFalse(deleteResult.Success);
            Assert.AreEqual(deleteResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(deleteResult.Value.Key(), documents[0].Key());
            Assert.AreEqual(deleteResult.Value.Rev(), documents[0].Rev());
        }

        [Test()]
        public async Task Should_delete_document_with_returnOld()
        {
            var documents = await Database.ClearCollectionAndFetchTestDocumentDataAsync(Database.TestDocumentCollectionName);
            var db = new ADatabase(Database.Alias);

            var deleteResult = await db.Document
                .ReturnOld()
                .DeleteAsync(documents[0].ID());

            Assert.AreEqual(202, deleteResult.StatusCode);
            Assert.IsTrue(deleteResult.Success);
            Assert.AreEqual(deleteResult.Value.ID(), documents[0].ID());
            Assert.AreEqual(deleteResult.Value.Key(), documents[0].Key());
            Assert.AreEqual(deleteResult.Value.Rev(), documents[0].Rev());
            Assert.IsTrue(deleteResult.Value.Has("old"));
        }

        #endregion

        public void Dispose()
        {
            Database.DeleteTestDatabaseAsync(Database.TestDatabaseGeneral).Wait();
        }
    }
}
