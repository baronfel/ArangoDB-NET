using System.Collections.Generic;
using NUnit.Framework;
using Arango.Client;
using System.Threading.Tasks;

namespace Arango.Tests
{
    [Ignore]
    [TestFixture()]
    public class FoxxOperationsTests
    {
        [Test()]
        public async Task Should_execute_get_foxx_requestAsync()
        {
            var db = new ADatabase(Database.SystemAlias);
            var getResult = await db.Foxx.Get<Dictionary<string, object>>("/getting-started/hello-world");

            Assert.AreEqual(200, getResult.StatusCode);
            Assert.AreEqual("bar", getResult.Value.String("foo"));
        }

        [Test()]
        public async Task Should_execute_post_foxx_request_with_bodyAsync()
        {
            var db = new ADatabase(Database.SystemAlias);

            var body = Dictator.New()
                .String("foo", "some string");

            var postResult = await db.Foxx
                .Body(body)
                .Post<Dictionary<string, object>>("/getting-started/hello-world");

            Assert.AreEqual(200, postResult.StatusCode);
            Assert.AreEqual(body.String("foo"), postResult.Value.String("foo"));
        }
    }
}
