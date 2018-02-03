using System.Collections.Generic;
using System.Threading.Tasks;
using Arango.Client.Protocol;
using Arango.fastJSON;

namespace Arango.Client
{
    public class ADatabase
    {
        readonly Dictionary<string, object> _parameters = new Dictionary<string, object>();
        readonly Connection _connection;
        
        #region Parameters
        
        /// <summary>
        /// Determines whether system collections should be excluded from the result.
        /// </summary>
        public ADatabase ExcludeSystem(bool value)
        {
            // string because value will be stored in query string
            _parameters.String(ParameterName.ExcludeSystem, value.ToString().ToLower());
        	
        	return this;
        }
        
        #endregion
        
        /// <summary>
        /// Provides access to collection operations in current database context.
        /// </summary>
        public ACollection Collection
        {
            get
            {
                return new ACollection(_connection);
            }
        }
        
        /// <summary>
        /// Provides access to document operations in current database context.
        /// </summary>
        public ADocument Document
        {
            get
            {
                return new ADocument(_connection);
            }
        }

        /// <summary>
        /// Provides access to AQL user function management operations in current database context.
        /// </summary>
        public AFunction Function
        {
            get
            {
                return new AFunction(_connection);
            }
        }
        
        /// <summary>
        /// Provides access to index operations in current database context.
        /// </summary>
        public AIndex Index
        {
            get
            {
                return new AIndex(_connection);
            }
        }
        
        /// <summary>
        /// Provides access to query operations in current database context.
        /// </summary>
        public AQuery Query
        {
            get
            {
                return new AQuery(_connection);
            }
        }
        
        /// <summary>
        /// Provides access to transaction operations in current database context.
        /// </summary>
        public ATransaction Transaction
        {
            get
            {
                return new ATransaction(_connection);
            }
        }

        /// <summary>
        /// Provides access to foxx services in current database context.
        /// </summary>
        public AFoxx Foxx
        {
            get
            {
                return new AFoxx(_connection);
            }
        }

        /// <summary>
        /// Initializes new database context to perform operations on remote database identified by specified alias.
        /// </summary>
        public ADatabase(string alias)
        {
            _connection = ASettings.GetConnection(alias);
        }

        #region Create database (POST)

        /// <summary>
        /// Creates new database with given name.
        /// </summary>
        public Task<AResult<bool>> CreateAsync(string databaseName) => CreateAsync(databaseName, null);

        /// <summary>
        /// Creates new database with given name and user list.
        /// </summary>
        public async Task<AResult<bool>> CreateAsync(string databaseName, List<AUser> users)
        {
            var request = new Request(HttpMethod.POST, ApiBaseUri.Database, "");
            var bodyDocument = new Dictionary<string, object>();
            
            // required: database name
            bodyDocument.String("name", databaseName);
            
            // optional: list of users
            if ((users != null) && (users.Count > 0))
            {
                var userList = new List<Dictionary<string, object>>();
                
                foreach (var user in users)
                {
                    var userItem = new Dictionary<string, object>();
                    
                    if (!string.IsNullOrEmpty(user.Username))
                    {
                        userItem.String("username", user.Username);
                    }
                    
                    if (!string.IsNullOrEmpty(user.Password))
                    {
                        userItem.String("passwd", user.Password);
                    }
                    
                    userItem.Bool("active", user.Active);
                    
                    if (user.Extra != null)
                    {
                        userItem.Document("extra", user.Extra);
                    }
                    
                    userList.Add(userItem);
                }
                
                bodyDocument.List("users", userList);
            }
            
            request.Body = JSON.ToJSON(bodyDocument, ASettings.JsonParameters);
            
            var response = await _connection.Send(request);
            var result = new AResult<bool>(response);
            
            switch (response.StatusCode)
            {
                case 201:
                    var body = response.ParseBody<Body<bool>>();
                    
                    result.Success = (body != null);
                    result.Value = body.Result;
                    break;
                case 400:
                case 403:
                case 404:
                default:
                    // Arango error
                    break;
            }
            
            _parameters.Clear();
            
            return result;
        }
        
        #endregion
        
        #region Get current database (GET)
        
        /// <summary>
        /// Retrieves information about currently connected database.
        /// </summary>
        public async Task<AResult<Dictionary<string, object>>> GetCurrentAsync()
        {
            var request = new Request(HttpMethod.GET, ApiBaseUri.Database, "/current");
            
            var response = await _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Body<Dictionary<string, object>>>();
                    
                    result.Success = (body != null);
                    result.Value = body.Result;
                    break;
                case 400:
                case 404:
                default:
                    // Arango error
                    break;
            }
            
            _parameters.Clear();
            
            return result;
        }
        
        #endregion
        
        #region Get list of accessible databases (GET)
        
        /// <summary>
        /// Retrieves list of accessible databases which current user can access without specifying a different username or password.
        /// </summary>
        public async Task<AResult<List<string>>> GetAccessibleDatabasesAsync()
        {
            var request = new Request(HttpMethod.GET, ApiBaseUri.Database, "/user");
            
            var response = await _connection.Send(request);
            var result = new AResult<List<string>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Body<List<string>>>();
                    
                    result.Success = (body != null);
                    result.Value = body.Result;
                    break;
                case 400:
                default:
                    // Arango error
                    break;
            }
            
            _parameters.Clear();
            
            return result;
        }
        
        #endregion
        
        #region Get list of all databases (GET)
        
        /// <summary>
        /// Retrieves the list of all existing databases.
        /// </summary>
        public async Task<AResult<List<string>>> GetAllDatabasesAsync()
        {
            var request = new Request(HttpMethod.GET, ApiBaseUri.Database, "");
            
            var response = await _connection.Send(request);
            var result = new AResult<List<string>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Body<List<string>>>();
                    
                    result.Success = (body != null);
                    result.Value = body.Result;
                    break;
                case 400:
                case 403:
                default:
                    // Arango error
                    break;
            }
            
            _parameters.Clear();
            
            return result;
        }
        
        #endregion
        
        #region Get list of all collections (GET)
        
        /// <summary>
        /// Retrieves information about collections in current database connection.
        /// </summary>
        public async Task<AResult<List<Dictionary<string, object>>>> GetAllCollectionsAsync()
        {
            var request = new Request(HttpMethod.GET, ApiBaseUri.Collection, "");
            
            // optional
            request.TrySetQueryStringParameter(ParameterName.ExcludeSystem, _parameters);
            
            var response = await _connection.Send(request);
            var result = new AResult<List<Dictionary<string, object>>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();
                    
                    result.Success = (body != null);
                    result.Value = body.List<Dictionary<string, object>>("result");
                    break;
                case 400:
                case 403:
                default:
                    // Arango error
                    break;
            }
            
            _parameters.Clear();
            
            return result;
        }
        
        #endregion
        
        #region Drop database (DELETE)
        
        /// <summary>
        /// Deletes specified database.
        /// </summary>
        public async Task<AResult<bool>> DropAsync(string databaseName)
        {
            var request = new Request(HttpMethod.DELETE, ApiBaseUri.Database, "/" + databaseName);
            
            var response = await _connection.Send(request);
            var result = new AResult<bool>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Body<bool>>();
                    
                    result.Success = (body != null);
                    result.Value = body.Result;
                    break;
                case 400:
                case 403:
                case 404:
                default:
                    // Arango error
                    break;
            }
            
            _parameters.Clear();
            
            return result;
        }
        
        #endregion
    }
}
