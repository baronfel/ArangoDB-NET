using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Arango.Client;
using System.Threading.Tasks;

namespace Arango.Tests
{
    [TestFixture()]
    public class IssueTests : IDisposable
    {
        public IssueTests()
		{
			Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral).Wait();
		}
        
        [Test()]
        public async Task Issue_No8_Guid_conversion()
        {
            await Database.CreateTestCollectionAsync(Database.TestDocumentCollectionName, ACollectionType.Document);
            var db = new ADatabase(Database.Alias);
            
            var demo = new IssueNo8Entity();
            demo.SomeOtherId = Guid.NewGuid();
            demo.Name = "My name";
            
            var createResult = await db.Document.CreateAsync(Database.TestDocumentCollectionName, demo);
            
            Assert.IsTrue(createResult.Success);
            
            var getresult = await db.Document.GetAsync<IssueNo8Entity>(createResult.Value.ID());
            
            Assert.IsTrue(getresult.Success);
            Assert.AreEqual(demo.SomeOtherId.ToString(), getresult.Value.SomeOtherId.ToString());
        }
        
        [Test()]
        public async Task Issue_No9_Enum_type_handling()
        {
            await Database.CreateTestCollectionAsync(Database.TestDocumentCollectionName, ACollectionType.Document);
            var db = new ADatabase(Database.Alias);
            
            var demo = new IssueNo9Entity();
            demo.SomeOtherId = Guid.NewGuid();
            demo.Name = "My name";
            demo.MyFavoriteColor = IssueNo9Entity.Color.Blue;
            
            var createResult = await db.Document.CreateAsync(Database.TestDocumentCollectionName, demo);
        
            Assert.IsTrue(createResult.Success);
            
            var getResult = await db.Document.GetAsync<IssueNo9Entity>(createResult.Value.ID());
            
            Assert.IsTrue(getResult.Success);
            Assert.AreEqual(demo.MyFavoriteColor, getResult.Value.MyFavoriteColor);
            
            var getDocResult = await db.Document.GetAsync<Dictionary<string, object>>(createResult.Value.ID());
            
            Assert.IsTrue(getDocResult.Success);
            Assert.IsTrue(getDocResult.Value.IsString("MyFavoriteColor"));
            Assert.AreEqual(demo.MyFavoriteColor.ToString(), getDocResult.Value.String("MyFavoriteColor"));
            
            // change JSON serialization options to serialize enum types as values (integers and not strings)
            ASettings.JsonParameters.UseValuesOfEnums = true;
            
            var createResult2 = await db.Document.CreateAsync(Database.TestDocumentCollectionName, demo);
            
            Assert.IsTrue(createResult2.Success);
            
            var getDocResult2 = await db.Document.GetAsync<Dictionary<string, object>>(createResult2.Value.ID());
            
            Assert.IsTrue(getDocResult2.Success);
            Assert.IsTrue(getDocResult2.Value.IsLong("MyFavoriteColor"));
            Assert.AreEqual((int)demo.MyFavoriteColor, getDocResult2.Value.Int("MyFavoriteColor"));
        }
        
        [Test()]
        public async Task Issue_No15_List_save_and_retrieve()
        {
            await Database.CreateTestCollectionAsync(Database.TestDocumentCollectionName, ACollectionType.Document);
            var db = new ADatabase(Database.Alias);
            
            var entity = new IssueNo15Entity();
            entity.ListNumbers = new List<int> { 1, 2, 3 };
            entity.ArrayNumbers = new int[] { 4, 5, 6};
            
            var createResult = await db.Document.CreateAsync(Database.TestDocumentCollectionName, entity);
            
            Assert.IsTrue(createResult.Success);
            
            var getresult = await db.Document.GetAsync<IssueNo15Entity>(createResult.Value.ID());
            
            Assert.IsTrue(getresult.Success);
            Assert.IsTrue(getresult.HasValue);
            
            for (int i = 0; i < getresult.Value.ListNumbers.Count; i++)
            {
                Assert.AreEqual(entity.ListNumbers[i], getresult.Value.ListNumbers[i]);
            }
            
            for (int i = 0; i < getresult.Value.ArrayNumbers.Length; i++)
            {
                Assert.AreEqual(entity.ArrayNumbers[i], getresult.Value.ArrayNumbers[i]);
            }
        }
        
        [Test()]
        public async Task Issue_No16_SortedList()
        {
            await Database.CreateTestCollectionAsync(Database.TestDocumentCollectionName, ACollectionType.Document);
            var db = new ADatabase(Database.Alias);
            
            var entity = new IssueNo16Entity();
            entity.SortedList = new SortedList<int, bool>();
            entity.SortedList.Add(1, true);
            entity.SortedList.Add(2, false);
            entity.SortedList.Add(3, false);
            entity.SortedList.Add(4, false);
            
            var createResult = await db.Document.CreateAsync(Database.TestDocumentCollectionName, entity);
            
            Assert.IsTrue(createResult.Success);
            
            var getResult = await db.Document.GetAsync<IssueNo16Entity>(createResult.Value.ID());
            
            Assert.IsTrue(getResult.Success);
            Assert.IsTrue(getResult.HasValue);
            
            for (int i = 0; i < getResult.Value.SortedList.Count; i++)
            {
                Assert.AreEqual(entity.SortedList.ElementAt(i).Key, getResult.Value.SortedList.ElementAt(i).Key);
                Assert.AreEqual(entity.SortedList.ElementAt(i).Value, getResult.Value.SortedList.ElementAt(i).Value);
            }
        }

        [Test()]
        public async Task Issue_No34_MapAttributesToProperties()
        {
            await Database.CreateTestCollectionAsync(Database.TestDocumentCollectionName, ACollectionType.Document);
            await Database.CreateTestCollectionAsync(Database.TestEdgeCollectionName, ACollectionType.Edge);
            var db = new ADatabase(Database.Alias);

            var vertex1 = new IssueNo34Entity
            {
                Key = "5",
                Foo = "some string value",
                Bar = 12345
            };

            var createResult1 = await db.Document.CreateAsync(Database.TestDocumentCollectionName, vertex1);

            Assert.IsTrue(createResult1.Success);
            Assert.IsTrue(createResult1.HasValue);
            Assert.AreEqual(vertex1.Key, createResult1.Value.Key());

            var getResult1 = await db.Document.GetAsync<IssueNo34Entity>(createResult1.Value.ID());

            Assert.IsTrue(getResult1.Success);
            Assert.IsTrue(getResult1.HasValue);
            Assert.AreEqual(vertex1.Key, getResult1.Value.Key);
            Assert.AreEqual(vertex1.Foo, getResult1.Value.Foo);
            Assert.AreEqual(vertex1.Bar, getResult1.Value.Bar);

            var vertex2 = new IssueNo34Entity
            {
                Key = "8",
                Foo = "some other string value",
                Bar = 67890
            };

            var createResult2 = await db.Document.CreateAsync(Database.TestDocumentCollectionName, vertex2);

            Assert.IsTrue(createResult2.Success);
            Assert.IsTrue(createResult2.HasValue);
            Assert.AreEqual(vertex2.Key, createResult2.Value.Key());

            var getResult2 = await db.Document.GetAsync<IssueNo34Entity>(createResult2.Value.ID());

            Assert.IsTrue(getResult2.Success);
            Assert.IsTrue(getResult2.HasValue);
            Assert.AreEqual(vertex2.Key, getResult2.Value.Key);
            Assert.AreEqual(vertex2.Foo, getResult2.Value.Foo);
            Assert.AreEqual(vertex2.Bar, getResult2.Value.Bar);

            var edge = new IssueNo34Entity
            {
                From = createResult1.Value.ID(),
                To = createResult2.Value.ID(),
                Key = "10",
                Foo = "edge string value",
                Bar = 13579
            };

            var createEdge = await db
                .Document
                .ReturnNew()
                .CreateEdgeAsync(Database.TestEdgeCollectionName, edge.From, edge.To, edge);

            Assert.IsTrue(createEdge.Success);
            Assert.IsTrue(createEdge.HasValue);
            Assert.AreEqual(edge.Key, createEdge.Value.Key());

            var getEdge = await db.Document.GetAsync<IssueNo34Entity>(createEdge.Value.ID());

            Assert.IsTrue(getEdge.Success);
            Assert.IsTrue(getEdge.HasValue);
            Assert.AreEqual(edge.From, getEdge.Value.From);
            Assert.AreEqual(edge.To, getEdge.Value.To);
            Assert.AreEqual(edge.Key, getEdge.Value.Key);
            Assert.AreEqual(edge.Foo, getEdge.Value.Foo);
            Assert.AreEqual(edge.Bar, getEdge.Value.Bar);

            var queryVertex1Result = await db.Query
                .Aql($"FOR item IN {Database.TestDocumentCollectionName} FILTER item._key == \"{vertex1.Key}\" RETURN item")
                .ToObjectAsync<IssueNo34Entity>();

            Assert.IsTrue(queryVertex1Result.Success);
            Assert.IsTrue(queryVertex1Result.HasValue);
            Assert.AreEqual(vertex1.Key, queryVertex1Result.Value.Key);
            Assert.AreEqual(vertex1.Foo, queryVertex1Result.Value.Foo);
            Assert.AreEqual(vertex1.Bar, queryVertex1Result.Value.Bar);

            var queryVertex2Result = await db.Query
                .Aql($"FOR item IN {Database.TestDocumentCollectionName} FILTER item._key == \"{vertex2.Key}\" RETURN item")
                .ToObjectAsync<IssueNo34Entity>();

            Assert.IsTrue(queryVertex2Result.Success);
            Assert.IsTrue(queryVertex2Result.HasValue);
            Assert.AreEqual(vertex2.Key, queryVertex2Result.Value.Key);
            Assert.AreEqual(vertex2.Foo, queryVertex2Result.Value.Foo);
            Assert.AreEqual(vertex2.Bar, queryVertex2Result.Value.Bar);

            var queryEdgeResult = await db.Query
                .Aql($"FOR item IN {Database.TestEdgeCollectionName} FILTER item._key == \"{edge.Key}\" RETURN item")
                .ToObjectAsync<IssueNo34Entity>();

            Assert.IsTrue(queryEdgeResult.Success);
            Assert.IsTrue(queryEdgeResult.HasValue);
            Assert.AreEqual(edge.From, queryEdgeResult.Value.From);
            Assert.AreEqual(edge.To, queryEdgeResult.Value.To);
            Assert.AreEqual(edge.Key, queryEdgeResult.Value.Key);
            Assert.AreEqual(edge.Foo, queryEdgeResult.Value.Foo);
            Assert.AreEqual(edge.Bar, queryEdgeResult.Value.Bar);
        }

        public void Dispose()
        {
            Database.DeleteTestDatabaseAsync(Database.TestDatabaseGeneral).Wait();
        }
    }
}
