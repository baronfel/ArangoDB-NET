using System.Collections.Generic;
using System.Threading.Tasks;
using Arango.Client.Protocol;
using Arango.fastJSON;

namespace Arango.Client
{
    public class AFoxx
    {
        readonly Dictionary<string, object> _parameters = new Dictionary<string, object>();
        readonly Connection _connection;

        internal AFoxx(Connection connection)
        {
            _connection = connection;
        }

        #region Parameters

        /// <summary>
        /// Serializes specified value as JSON object into request body.
        /// </summary>
        public AFoxx Body(object value)
        {
            _parameters.Object(ParameterName.Body, value);

            return this;
        }

        #endregion

        /// <summary>
        /// Sends GET request to specified foxx service location.
        /// </summary>
        public Task<AResult<T>> Get<T>(string relativeUri) => RequestAsync<T>(HttpMethod.GET, relativeUri);

        /// <summary>
        /// Sends POST request to specified foxx service location.
        /// </summary>
        public Task<AResult<T>> Post<T>(string relativeUri) => RequestAsync<T>(HttpMethod.POST, relativeUri);

        /// <summary>
        /// Sends PUT request to specified foxx service location.
        /// </summary>
        public Task<AResult<T>> Put<T>(string relativeUri) => RequestAsync<T>(HttpMethod.PUT, relativeUri);

        /// <summary>
        /// Sends PATCH request to specified foxx service location.
        /// </summary>
        public Task<AResult<T>> PatchAsync<T>(string relativeUri) => RequestAsync<T>(HttpMethod.PATCH, relativeUri);

        /// <summary>
        /// Sends DELETE request to specified foxx service location.
        /// </summary>
        public Task<AResult<T>> DeleteAsync<T>(string relativeUri) => RequestAsync<T>(HttpMethod.DELETE, relativeUri);

        private async Task<AResult<T>> RequestAsync<T>(HttpMethod httpMethod, string relativeUri)
        {
            var request = new Request(httpMethod, relativeUri);

            if (_parameters.Has(ParameterName.Body))
            {
                request.Body = JSON.ToJSON(_parameters.Object(ParameterName.Body), ASettings.JsonParameters);
            }

            var response = await _connection.Send(request);
            var result = new AResult<T>(response);

            result.Value = response.ParseBody<T>();

            _parameters.Clear();

            return result;
        }
    }
}
