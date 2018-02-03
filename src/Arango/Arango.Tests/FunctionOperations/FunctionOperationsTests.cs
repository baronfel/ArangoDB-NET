using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Arango.Client;
using System.Threading.Tasks;

namespace Arango.Tests
{
    [TestFixture()]
    public class FunctionOperationsTests : IDisposable
    {
        [Test()]
        public async Task Should_register_functionAsync()
        {
        	await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);
            var db = new ADatabase(Database.Alias);
            
            var registerResult = await db.Function.RegisterAsync(
                "myfunctions::temperature::celsiustofahrenheit", 
                "function (celsius) { return celsius * 1.8 + 32; }"
            );
            
            Assert.AreEqual(201, registerResult.StatusCode);
            Assert.IsTrue(registerResult.Success);
            Assert.IsTrue(registerResult.HasValue);
            Assert.IsTrue(registerResult.Value);
            
            const int celsius = 30;
            const float fahrenheit = celsius * 1.8f + 32;
            
            var queryResult = await db.Query
                .BindVar("celsius", celsius)
                .Aql("return myfunctions::temperature::celsiustofahrenheit(@celsius)")
                .ToListAsync<float>();
            
            Assert.AreEqual(fahrenheit, queryResult.Value.First());
        }
        
        [Test()]
        public async Task Should_list_functionsAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);
            var db = new ADatabase(Database.Alias);
            
            const string name1 = "myfunctions::temperature::celsiustofahrenheit1";
            const string code1 = "function (celsius) { return celsius * 1.8 + 40; }";
            var registerResult1 = await db.Function.RegisterAsync(name1, code1);
            
            Assert.AreEqual(true, registerResult1.Success);
            
            const string name2 = "myfunctions::temperature::celsiustofahrenheit2";
            const string code2 = "function (celsius) { return celsius * 1.8 + 32; }";
            var registerResult2 = await db.Function.RegisterAsync(name2, code2);
            
            Assert.AreEqual(true, registerResult2.Success);
            
            var listResult = await db.Function.ListAsync();
            
            Assert.AreEqual(200, listResult.StatusCode);
            Assert.IsTrue(listResult.Success);
            Assert.IsTrue(listResult.HasValue);
            Assert.AreEqual(2, listResult.Value.Count);
            // retrieved order of the functions seems to be reversed
            Assert.AreEqual(name2, listResult.Value[0].String("name"));
            Assert.AreEqual(code2, listResult.Value[0].String("code"));
            Assert.AreEqual(name1, listResult.Value[1].String("name"));
            Assert.AreEqual(code1, listResult.Value[1].String("code"));
        }
        
        [Test()]
        public async Task Should_replace_functionAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);
            var db = new ADatabase(Database.Alias);
            
            var registerResult = await db.Function.RegisterAsync(
                "myfunctions::temperature::celsiustofahrenheit", 
                "function (celsius) { return celsius * 1.8 + 40; }"
            );
            
            Assert.AreEqual(201, registerResult.StatusCode);
            Assert.IsTrue(registerResult.Success);
            Assert.IsTrue(registerResult.HasValue);
            Assert.IsTrue(registerResult.Value);
            
            var replaceResult = await db.Function.RegisterAsync(
                "myfunctions::temperature::celsiustofahrenheit", 
                "function (celsius) { return celsius * 1.8 + 32; }"
            );
            
            Assert.AreEqual(200, replaceResult.StatusCode);
            Assert.IsTrue(replaceResult.Success);
            Assert.IsTrue(replaceResult.HasValue);
            Assert.IsTrue(replaceResult.Value);
            
            const int celsius = 30;
            const float fahrenheit = celsius * 1.8f + 32;
            
            var queryResult = await db.Query
                .BindVar("celsius", celsius)
                .Aql("return myfunctions::temperature::celsiustofahrenheit(@celsius)")
                .ToListAsync<float>();
            
            Assert.AreEqual(fahrenheit, queryResult.Value.First());
        }
        
        [Test()]
        public async Task Should_unregister_functionAsync()
        {
            await Database.CreateTestDatabaseAsync(Database.TestDatabaseGeneral);
            var db = new ADatabase(Database.Alias);
            
            var registerResult = await db.Function.RegisterAsync(
                "myfunctions::temperature::celsiustofahrenheit", 
                "function (celsius) { return celsius * 1.8 + 40; }"
            );
            
            Assert.AreEqual(201, registerResult.StatusCode);
            Assert.IsTrue(registerResult.Success);
            Assert.IsTrue(registerResult.HasValue);
            Assert.IsTrue(registerResult.Value);
            
            var unregisterResult = await db.Function.UnregisterAsync("myfunctions::temperature::celsiustofahrenheit");
            
            Assert.AreEqual(200, unregisterResult.StatusCode);
            Assert.IsTrue(unregisterResult.Success);
            Assert.IsTrue(unregisterResult.HasValue);
            Assert.IsTrue(unregisterResult.Value);
        }
        
        public void Dispose()
        {
            Database.DeleteTestDatabaseAsync(Database.TestDatabaseGeneral).Wait();
        }
    }
}
