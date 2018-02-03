using System.Collections.Generic;
using System.Threading.Tasks;
using Arango.Client.Protocol;
using Arango.fastJSON;

namespace Arango.Client
{
    public class ACollection
    {
        readonly Dictionary<string, object> _parameters = new Dictionary<string, object>();
        readonly Connection _connection;
        
        internal ACollection(Connection connection)
        {
            _connection = connection;
        }
        
        #region Parameters
        
        /// <summary>
        /// Determines type of the collection. Default value: Document.
        /// </summary>
        public ACollection Type(ACollectionType value)
        {
            // set enum format explicitely to override global setting
            _parameters.Enum(ParameterName.Type, value, EnumFormat.Object);
        	
        	return this;
        }
        
        /// <summary>
        /// Determines whether or not to wait until data are synchronised to disk. Default value: false.
        /// </summary>
        public ACollection WaitForSync(bool value)
        {
            _parameters.Bool(ParameterName.WaitForSync, value);
        	
        	return this;
        }
        
        /// <summary>
        /// Determines maximum size of a journal or datafile in bytes. Default value: server configured.
        /// </summary>
        public ACollection JournalSize(long value)
        {
            _parameters.Long(ParameterName.JournalSize, value);
        	
        	return this;
        }
        
        /// <summary>
        /// Determines whether the collection will be compacted. Default value: true.
        /// </summary>
        public ACollection DoCompact(bool value)
        {
            _parameters.Bool(ParameterName.DoCompact, value);
        	
        	return this;
        }
        
        /// <summary>
        /// Determines whether the collection is a system collection. Default value: false.
        /// </summary>
        public ACollection IsSystem(bool value)
        {
            _parameters.Bool(ParameterName.IsSystem, value);
        	
        	return this;
        }
        
        /// <summary>
        /// Determines whether the collection data is kept in-memory only and not made persistent. Default value: false.
        /// </summary>
        public ACollection IsVolatile(bool value)
        {
            _parameters.Bool(ParameterName.IsVolatile, value);
        	
        	return this;
        }
        
        #region Key options
        
        /// <summary>
        /// Determines the type of the key generator. Default value: Traditional.
        /// </summary>
        public ACollection KeyGeneratorType(AKeyGeneratorType value)
        {
            // needs to be in string format - set enum format explicitely to override global setting
            _parameters.Enum(ParameterName.KeyOptionsType, value.ToString().ToLower(), EnumFormat.String);
        	
        	return this;
        }
        
        /// <summary>
        /// Determines whether it is allowed to supply custom key values in the _key attribute of a document. Default value: true.
        /// </summary>
        public ACollection AllowUserKeys(bool value)
        {
            _parameters.Bool(ParameterName.KeyOptionsAllowUserKeys, value);
        	
        	return this;
        }
        
        /// <summary>
        /// Determines increment value for autoincrement key generator.
        /// </summary>
        public ACollection KeyIncrement(long value)
        {
            _parameters.Long(ParameterName.KeyOptionsIncrement, value);
        	
        	return this;
        }
        
        /// <summary>
        /// Determines initial offset value for autoincrement key generator.
        /// </summary>
        public ACollection KeyOffset(long value)
        {
            _parameters.Long(ParameterName.KeyOptionsOffset, value);
        	
        	return this;
        }
        
        #endregion
        
        /// <summary>
        /// Determines the number of shards to create for the collection in cluster environment. Default value: 1.
        /// </summary>
        public ACollection NumberOfShards(int value)
        {
            _parameters.Int(ParameterName.NumberOfShards, value);
        	
        	return this;
        }
        
        /// <summary>
        /// Determines which document attributes are used to specify the target shard for documents in cluster environment. Default value: ["_key"].
        /// </summary>
        public ACollection ShardKeys(List<string> value)
        {
            _parameters.List(ParameterName.ShardKeys, value);
        	
        	return this;
        }
        
        /// <summary>
        /// Determines whether the return value should include the number of documents in collection. Default value: true.
        /// </summary>
        public ACollection Count(bool value)
        {
            _parameters.Bool(ParameterName.Count, value);
        	
        	return this;
        }
        
        #region Checksum options
        
        /// <summary>
        /// Determines whether to include document revision ids in the checksum calculation. Default value: false.
        /// </summary>
        public ACollection WithRevisions(bool value)
        {
            // needs to be in string format
            _parameters.String(ParameterName.WithRevisions, value.ToString().ToLower());
        	
        	return this;
        }
        
        /// <summary>
        /// Determines whether to include document body data in the checksum calculation. Default value: false.
        /// </summary>
        public ACollection WithData(bool value)
        {
            // needs to be in string format
            _parameters.String(ParameterName.WithData, value.ToString().ToLower());
        	
        	return this;
        }
        
        #endregion
        
        /// <summary>
        /// Determines which attribute will be retuned in the list. Default value: Path.
        /// </summary>
        public ACollection ReturnListType(AReturnListType value)
        {
            // needs to be string value
            _parameters.String(ParameterName.Type, value.ToString().ToLower());
        	
        	return this;
        }
        
        #endregion
        
        #region Create collection (POST)
        
        /// <summary>
        /// Creates new collection in current database context.
        /// </summary>
        public async Task<AResult<Dictionary<string, object>>> CreateAsync(string collectionName)
        {
            var request = new Request(HttpMethod.POST, ApiBaseUri.Collection, "");
            var bodyDocument = new Dictionary<string, object>();
            
            // required
            bodyDocument.String(ParameterName.Name, collectionName);
            // optional
            Request.TrySetBodyParameter(ParameterName.Type, _parameters, bodyDocument);
            // optional
            Request.TrySetBodyParameter(ParameterName.WaitForSync, _parameters, bodyDocument);
            // optional
            Request.TrySetBodyParameter(ParameterName.JournalSize, _parameters, bodyDocument);
            // optional
            Request.TrySetBodyParameter(ParameterName.DoCompact, _parameters, bodyDocument);
            // optional
            Request.TrySetBodyParameter(ParameterName.IsSystem, _parameters, bodyDocument);
            // optional
            Request.TrySetBodyParameter(ParameterName.IsVolatile, _parameters, bodyDocument);
            // optional
            Request.TrySetBodyParameter(ParameterName.KeyOptionsType, _parameters, bodyDocument);
            // optional
            Request.TrySetBodyParameter(ParameterName.KeyOptionsAllowUserKeys, _parameters, bodyDocument);
            // optional
            Request.TrySetBodyParameter(ParameterName.KeyOptionsIncrement, _parameters, bodyDocument);
            // optional
            Request.TrySetBodyParameter(ParameterName.KeyOptionsOffset, _parameters, bodyDocument);
            // optional
            Request.TrySetBodyParameter(ParameterName.NumberOfShards, _parameters, bodyDocument);
            // optional
            Request.TrySetBodyParameter(ParameterName.ShardKeys, _parameters, bodyDocument);
            
            request.Body = JSON.ToJSON(bodyDocument, ASettings.JsonParameters);
            
            var response = await _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();
                    
                    result.Success = (body != null);
                    result.Value = body;
                    break;
                default:
                    // Arango error
                    break;
            }
            
            _parameters.Clear();
            
            return result;
        }
        
        #endregion
        
        #region Get collection (GET)
        
        /// <summary>
        /// Retrieves basic information about specified collection.
        /// </summary>
        public async Task<AResult<Dictionary<string, object>>> GetAsync(string collectionName)
        {
            var request = new Request(HttpMethod.GET, ApiBaseUri.Collection, "/" + collectionName);

            var response = await _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();
                    
                    result.Success = (body != null);
                    result.Value = body;
                    break;
                case 404:
                default:
                    // Arango error
                    break;
            }
            
            _parameters.Clear();
            
            return result;
        }
        
        #endregion
        
        #region Get collection properties (GET)
        
        /// <summary>
        /// Retrieves basic information with additional properties about specified collection.
        /// </summary>
        public async Task<AResult<Dictionary<string, object>>> GetPropertiesAsync(string collectionName)
        {
            var request = new Request(HttpMethod.GET, ApiBaseUri.Collection, "/" + collectionName + "/properties");

            var response = await _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();
                    
                    result.Success = (body != null);
                    result.Value = body;
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
        
        #region Get collection documents count (GET)
        
        /// <summary>
        /// Retrieves basic information with additional properties and document count in specified collection.
        /// </summary>
        public async Task<AResult<Dictionary<string, object>>> GetCountAsync(string collectionName)
        {
            var request = new Request(HttpMethod.GET, ApiBaseUri.Collection, "/" + collectionName + "/count");

            var response = await _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();
                    
                    result.Success = (body != null);
                    result.Value = body;
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
        
        #region Get collection figures (GET)
        
        /// <summary>
        /// Retrieves basic information with additional properties, document count and figures in specified collection.
        /// </summary>
        public async Task<AResult<Dictionary<string, object>>> GetFiguresAsync(string collectionName)
        {
            var request = new Request(HttpMethod.GET, ApiBaseUri.Collection, "/" + collectionName + "/figures");

            var response = await _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();
                    
                    result.Success = (body != null);
                    result.Value = body;
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
        
        #region Get collection revision (GET)
        
        /// <summary>
        /// Retrieves basic information and revision ID of specified collection.
        /// </summary>
        public async Task<AResult<Dictionary<string, object>>> GetRevisionAsync(string collectionName)
        {
            var request = new Request(HttpMethod.GET, ApiBaseUri.Collection, "/" + collectionName + "/revision");

            var response = await _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();
                    
                    result.Success = (body != null);
                    result.Value = body;
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
        
        #region Get collection checksum (GET)
        
        /// <summary>
        /// Retrieves basic information, revision ID and checksum of specified collection.
        /// </summary>
        public async Task<AResult<Dictionary<string, object>>> GetChecksumAsync(string collectionName)
        {
            var request = new Request(HttpMethod.GET, ApiBaseUri.Collection, "/" + collectionName + "/checksum");

            // optional
            request.TrySetQueryStringParameter(ParameterName.WithRevisions, _parameters);
            // optional
            request.TrySetQueryStringParameter(ParameterName.WithData, _parameters);
            
            var response = await  _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();
                    
                    result.Success = (body != null);
                    result.Value = body;
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
        
        #region Get all indexes (GET)
        
        /// <summary>
        /// Retrieves list of indexes in specified collection.
        /// </summary>
        public async Task<AResult<Dictionary<string, object>>> GetAllIndexesAsync(string collectionName)
        {
            var request = new Request(HttpMethod.GET, ApiBaseUri.Index, "");

            // required
            request.QueryString.Add(ParameterName.Collection, collectionName);
            
            var response = await _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();
                    
                    result.Success = (body != null);
                    result.Value = body;
                    break;
                case 404:
                default:
                    // Arango error
                    break;
            }
            
            _parameters.Clear();
            
            return result;
        }
        
        #endregion
        
        #region Truncate collection (PUT)
        
        /// <summary>
        /// Removes all documents from specified collection.
        /// </summary>
        public async Task<AResult<Dictionary<string, object>>> TruncateAsync(string collectionName)
        {
            var request = new Request(HttpMethod.PUT, ApiBaseUri.Collection, "/" + collectionName + "/truncate");
            
            var response = await _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();
                    
                    result.Success = (body != null);
                    result.Value = body;
                    break;
                default:
                    // Arango error
                    break;
            }
            
            _parameters.Clear();
            
            return result;
        }
        
        #endregion
        
        #region Load collection (PUT)
        
        /// <summary>
        /// Loads specified collection into memory.
        /// </summary>
        public async Task<AResult<Dictionary<string, object>>> LoadAsync(string collectionName)
        {
            var request = new Request(HttpMethod.PUT, ApiBaseUri.Collection, "/" + collectionName + "/load");
            
            if (_parameters.Has(ParameterName.Count))
            {
                var bodyDocument = new Dictionary<string, object>();
                
                // optional
                Request.TrySetBodyParameter(ParameterName.Count, _parameters, bodyDocument);
                
                request.Body = JSON.ToJSON(bodyDocument, ASettings.JsonParameters);
            }
            
            var response = await _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();
                    
                    result.Success = (body != null);
                    result.Value = body;
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
        
        #region Unload collection (PUT)
        
        /// <summary>
        /// Unloads specified collection from memory.
        /// </summary>
        public async Task<AResult<Dictionary<string, object>>> UnloadAsync(string collectionName)
        {
            var request = new Request(HttpMethod.PUT, ApiBaseUri.Collection, "/" + collectionName + "/unload");
            
            var response = await _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();
                    
                    result.Success = (body != null);
                    result.Value = body;
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
        
        #region Change collection properties (PUT)
        
        /// <summary>
        /// Changes properties of specified collection.
        /// </summary>
        public async Task<AResult<Dictionary<string, object>>> ChangePropertiesAsync(string collectionName)
        {
            var request = new Request(HttpMethod.PUT, ApiBaseUri.Collection, "/" + collectionName + "/properties");
            var bodyDocument = new Dictionary<string, object>();
            
            // optional
            Request.TrySetBodyParameter(ParameterName.WaitForSync, _parameters, bodyDocument);
            // optional
            Request.TrySetBodyParameter(ParameterName.JournalSize, _parameters, bodyDocument);
            
            request.Body = JSON.ToJSON(bodyDocument, ASettings.JsonParameters);
            
            var response = await _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();
                    
                    result.Success = (body != null);
                    result.Value = body;
                    break;
                default:
                    // Arango error
                    break;
            }
            
            _parameters.Clear();
            
            return result;
        }
        
        #endregion
        
        #region Rename collection (PUT)
        
        /// <summary>
        /// Renames specified collection.
        /// </summary>
        public async Task<AResult<Dictionary<string, object>>> RenameAsync(string collectionName, string newCollectionName)
        {
            var request = new Request(HttpMethod.PUT, ApiBaseUri.Collection, "/" + collectionName + "/rename");
            var bodyDocument = new Dictionary<string, object>()
                .String(ParameterName.Name, newCollectionName);
            
            request.Body = JSON.ToJSON(bodyDocument, ASettings.JsonParameters);
            
            var response = await _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();
                    
                    result.Success = (body != null);
                    result.Value = body;
                    break;
                default:
                    // Arango error
                    break;
            }
            
            _parameters.Clear();
            
            return result;
        }
        
        #endregion
        
        #region Rotate journal of a collection (PUT)
        
        /// <summary>
        /// Rotates the journal of specified collection to make the data in the file available for compaction. Current journal of the collection will be closed and turned into read-only datafile. This operation is not available in cluster environment.
        /// </summary>
        public async Task<AResult<bool>> RotateJournalAsync(string collectionName)
        {
            var request = new Request(HttpMethod.PUT, ApiBaseUri.Collection, "/" + collectionName + "/rotate");
            
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
                case 404:
                default:
                    // Arango error
                    break;
            }
            
            _parameters.Clear();
            
            return result;
        }
        
        #endregion
        
        #region Delete collection (DELETE)
        
        /// <summary>
        /// Deletes specified collection.
        /// </summary>
        public async Task<AResult<Dictionary<string, object>>> DeleteAsync(string collectionName)
        {
            var request = new Request(HttpMethod.DELETE, ApiBaseUri.Collection, "/" + collectionName);

            // optional
            request.TrySetQueryStringParameter(ParameterName.IsSystem, _parameters);

            var response = await _connection.Send(request);
            var result = new AResult<Dictionary<string, object>>(response);
            
            switch (response.StatusCode)
            {
                case 200:
                    var body = response.ParseBody<Dictionary<string, object>>();
                    
                    result.Success = (body != null);
                    result.Value = body;
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
    }
}
