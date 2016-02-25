// package net.butfly.albacore.utils.storage.swift;
//
// import java.io.IOException;
// import java.io.InputStream;
// import java.io.Serializable;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.Date;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.concurrent.ExecutionException;
//
// import
// net.butfly.albacore.helper.swift.exception.AuthenticationFailureException;
// import net.butfly.albacore.helper.swift.exception.OperationFailureException;
// import net.butfly.albacore.helper.swift.exception.UnknownResponseException;
// import net.butfly.albacore.utils.DateUtils;
// import net.butfly.albacore.utils.Texts;
// import net.butfly.albacore.utils.storage.swift.meta.ContainerMeta;
// import net.butfly.albacore.utils.storage.swift.meta.ObjectMeta;
//
// import org.apache.http.Header;
// import org.apache.http.HttpEntity;
// import org.apache.http.HttpHeaders;
// import org.apache.http.HttpResponse;
// import org.apache.http.entity.BasicHttpEntity;
// import org.apache.http.message.BasicHeader;
// import org.apache.http.protocol.HTTP;
// import org.apache.http.util.EntityUtils;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// import sun.net.www.http.HttpClient;
//
// import com.google.gson.Gson;
// import com.ning.http.client.AsyncHttpClient;
// import com.ning.http.client.AsyncHttpClient.BoundRequestBuilder;
// import com.ning.http.client.AsyncHttpClientConfig;
// import com.ning.http.client.Response;
// import com.ning.http.client.providers.netty.NettyAsyncHttpProvider;
//
// @Deprecated
// public class SwiftContext {
// private final static String STORAGE_USER_HEADER_NAME = "X-Storage-User";
// private final static String STORAGE_PASS_HEADER_NAME = "X-Storage-Pass";
// private final static String STORAGE_URL_HEADER_NAME = "X-Storage-Url";
// // private final static String AUTH_USER_HEADER_NAME = "X-Auth-User";
// // private final static String AUTH_KEY_HEADER_NAME = "X-Auth-Key";
// private final static String AUTH_TOKEN_HEADER_NAME = "X-Auth-Token";
// private final static String OBJECT_META_PREFIX = "X-Object-Meta-";
// private final static Logger logger =
// LoggerFactory.getLogger(SwiftContext.class);
//
// private Gson gson;
// private String serviceUrl = null;
// private String token = null;
//
// // for auto-auth after timeout.
// private String authUrl;
// private String password;
// private String username;
// private AsyncHttpClient client;
//
// protected SwiftContext() {
// this.authUrl = null;
// this.password = null;
// this.username = null;
// this.gson = new Gson();
// this.client = new AsyncHttpClient(new NettyAsyncHttpProvider(new
// AsyncHttpClientConfig.Builder().build()));
// }
//
// public SwiftContext(String authUrl, String user, String pass) throws
// AuthenticationFailureException {
// this.authUrl = authUrl;
// this.password = pass;
// this.username = user;
// this.login();
// this.gson = new Gson();
// }
//
// public void login() throws AuthenticationFailureException {
// Response resp;
// try {
// resp =
// this.client.prepareGet(this.authUrl).addHeader(STORAGE_USER_HEADER_NAME,
// this.username)
// .addHeader(STORAGE_PASS_HEADER_NAME, this.password).execute().get();
// } catch (InterruptedException e) {
// throw new AuthenticationFailureException(e);
// } catch (ExecutionException e) {
// throw new AuthenticationFailureException(e.getCause());
// }
// this.serviceUrl = resp.getHeader(STORAGE_URL_HEADER_NAME);
// this.token = resp.getHeader(AUTH_TOKEN_HEADER_NAME);
// }
//
// // Storage Accounts
//
// /**
// * Retrieve account metadata (HEAD account)
// *
// * @throws OperationFailureException
// * @throws UnknownResponseException
// * @throws AuthenticationFailureException
// */
// public ContainerMeta lsattr() throws OperationFailureException,
// UnknownResponseException, AuthenticationFailureException {
// Response resp;
// try {
// resp = this.client.prepareHead(this.serviceUrl).execute().get();
// } catch (InterruptedException e) {
// throw new OperationFailureException(e);
// } catch (ExecutionException e) {
// throw new OperationFailureException(e.getCause());
// }
// int c = this.handleStatusCode(resp, 204, 404);
// if (c == 404) throw new OperationFailureException("Swift account metadata
// failure for given container not found.");
// if (c != 204)
// throw new OperationFailureException("Swift account emtadata failurewith
// invalid response: " + resp.toString());
// ContainerMeta r = new ContainerMeta();
// try {
// r.setName(this.username);
// r.setBytes(Integer.parseInt(resp.getHeader("X-Account-Bytes-Used")));
// r.setCount(Integer.parseInt(resp.getHeader("X-Account-Container-Count")));
// } catch (NumberFormatException ex) {
// throw new UnknownResponseException(ex);
// }
// return r;
// }
//
// /**
// * List Containers (GET /account)
// *
// * @throws UnknownResponseException
// * @throws OperationFailureException
// * @throws AuthenticationFailureException
// */
// public String[] ls() throws OperationFailureException,
// UnknownResponseException, AuthenticationFailureException {
// return this.ls(new ListOption());
// }
//
// public String[] ls(ListOption option) throws OperationFailureException,
// UnknownResponseException,
// AuthenticationFailureException {
// if (null != option) option.setFormat(null);
// String r = this.getListResponse(null, option);
// return null == r ? new String[0] : r.split("\n");
// }
//
// public ContainerMeta[] ll() throws OperationFailureException,
// UnknownResponseException, AuthenticationFailureException {
// return this.ll(new ListOption());
// }
//
// public ContainerMeta[] ll(ListOption option) throws
// OperationFailureException, UnknownResponseException,
// AuthenticationFailureException {
// String r = this.getListResponse(null, null == option ? new ListOption() :
// option);
// return null == r ? new ContainerMeta[0] : this.gson.fromJson(r,
// ContainerMeta[].class);
// }
//
// // Storage Containers
// /**
// * Retrieve container metadata (HEAD /account/container)
// *
// * @throws OperationFailureException
// * @throws UnknownResponseException
// * @throws AuthenticationFailureException
// */
// public ContainerMeta lsattr(String container) throws
// OperationFailureException, UnknownResponseException,
// AuthenticationFailureException {
// Response resp;
// try {
// resp = this.client.prepareHead(this.serviceUrl + "/" +
// container).execute().get();
// } catch (InterruptedException e) {
// throw new OperationFailureException(e);
// } catch (ExecutionException e) {
// throw new OperationFailureException(e.getCause());
// }
// int c = this.handleStatusCode(resp, 204, 404);
// if (c == 404) throw new OperationFailureException("Swift container metadata
// failure for given container not found.");
// if (c != 204)
// throw new OperationFailureException("Swift container emtadata failurewith
// invalid response: " + resp.toString());
// ContainerMeta r = new ContainerMeta();
// r.setName(container);
// r.setCount(Integer.parseInt(resp.getHeader("X-Container-Object-Count")));
// r.setBytes(Integer.parseInt(resp.getHeader("X-Container-Bytes-Used")));
// return r;
// }
//
// /**
// * List objects (GET /account/container)
// *
// * @throws UnknownResponseException
// * @throws OperationFailureException
// * @throws AuthenticationFailureException
// */
// public String[] ls(String container) throws OperationFailureException,
// UnknownResponseException,
// AuthenticationFailureException {
// return this.ls(container, null);
// }
//
// public String[] ls(String container, ListOption option) throws
// OperationFailureException, UnknownResponseException,
// AuthenticationFailureException {
// if (null != option) option.setFormat(null);
// String r = this.getListResponse(container, option);
// return null == r ? new String[0] : r.split("\n");
// }
//
// public ObjectMeta[] ll(String container) throws OperationFailureException,
// UnknownResponseException,
// AuthenticationFailureException {
// return this.ll(container, null);
// }
//
// public ObjectMeta[] ll(String container, ListOption option) throws
// OperationFailureException, UnknownResponseException,
// AuthenticationFailureException {
// String r = this.getListResponse(container, null == option ? new ListOption()
// : option);
// return null == r ? new ObjectMeta[0] : this.gson.fromJson(r,
// ObjectMeta[].class);
// }
//
// /**
// * Create container (PUT /account/container)
// *
// * @throws UnknownResponseException
// * @throws OperationFailureException
// * @throws AuthenticationFailureException
// */
// public void mkdir(String container) throws OperationFailureException,
// UnknownResponseException,
// AuthenticationFailureException {
// Response resp;
// try {
// resp = this.client.preparePut(this.serviceUrl + "/" +
// container).execute().get();
// } catch (InterruptedException e) {
// throw new OperationFailureException(e);
// } catch (ExecutionException e) {
// throw new OperationFailureException(e.getCause());
// }
// int c = this.handleStatusCode(resp, 202);
// if (c == 202)
// throw new OperationFailureException("Swift container create failure for given
// container already existed.");
// if (c != 201)
// throw new OperationFailureException("Swift container create failurewith
// invalid response: " + resp.toString());
// }
//
// /**
// * Delete container (DELETE /account/container)
// *
// * @throws OperationFailureException
// * @throws UnknownResponseException
// * @throws AuthenticationFailureException
// */
// public void rmdir(String container) throws OperationFailureException,
// UnknownResponseException,
// AuthenticationFailureException {
// Response resp;
// try {
// resp = this.client.prepareDelete(this.serviceUrl + "/" +
// container).execute().get();
// } catch (InterruptedException e) {
// throw new OperationFailureException(e);
// } catch (ExecutionException e) {
// throw new OperationFailureException(e.getCause());
// }
// int c = this.handleStatusCode(resp, 204, 404, 409);
// if (c == 404) throw new OperationFailureException("Swift container remove
// failure for given container not found.");
// if (c == 409) throw new OperationFailureException("Swift container remove
// failure for given container is not empty.");
// if (c != 204)
// throw new OperationFailureException("Swift container remove failure with
// invalid response: " + resp.toString());
// }
//
// // Storage Objects
// /**
// * Retrieve object metadata (HEAD /account/container/object)
// *
// * @throws OperationFailureException
// * @throws UnknownResponseException
// * @throws AuthenticationFailureException
// */
// public Map<String, String> lsattr(String container, String object) throws
// OperationFailureException,
// UnknownResponseException, AuthenticationFailureException {
// Response resp;
// try {
// resp = this.client.prepareHead(this.serviceUrl + "/" + container + "/" +
// object).execute().get();
// } catch (InterruptedException e) {
// throw new OperationFailureException(e);
// } catch (ExecutionException e) {
// throw new OperationFailureException(e.getCause());
// }
// int c = this.handleStatusCode(resp, 200, 404);
// if (c == 404) throw new OperationFailureException("Swift object metadata
// failure for given object not found.");
// if (c != 200)
// throw new OperationFailureException("Swift container emtadata failurewith
// invalid response: " + resp.toString());
// Map<String, String> r = new HashMap<String, String>();
// for (String name : resp.getHeaders().keySet())
// if (name.startsWith(OBJECT_META_PREFIX)) {
// String n = name.substring(OBJECT_META_PREFIX.length());
// if (r.containsKey(n)) throw new OperationFailureException("Conflict object
// metadata key: " + n);
// r.put(n, resp.getHeader(name));
// }
// return r;
// }
//
// /**
// * Update object metadata (POST /account/container/object)
// *
// * @throws OperationFailureException
// * @throws UnknownResponseException
// * @throws AuthenticationFailureException
// */
// public void touch(String container, String object, Map<String, String>
// metadata) throws OperationFailureException,
// UnknownResponseException, AuthenticationFailureException {
// BoundRequestBuilder req = this.client.preparePost(this.serviceUrl + "/" +
// container + "/" + object);
// for (String name : metadata.keySet())
// req.addHeader(OBJECT_META_PREFIX + name, metadata.get(name));
// Response resp;
// try {
// resp = req.execute().get();
// } catch (InterruptedException e) {
// throw new OperationFailureException(e);
// } catch (ExecutionException e) {
// throw new OperationFailureException(e.getCause());
// }
// int c = this.handleStatusCode(resp, 202, 404);
// if (c == 404) throw new OperationFailureException("Swift object metadata
// failure for given object not found.");
// if (c != 202)
// throw new OperationFailureException("Swift object memtadata failure with
// invalid response: " + resp.toString());
// }
//
// /**
// * Retrieve object (GET /account/container/object) REMEMBER: Close the result
// InputStream after using...
// *
// * @throws OperationFailureException
// * @throws UnknownResponseException
// * @throws AuthenticationFailureException
// */
// public InputStream cat(String container, String object, FetchOption option)
// throws OperationFailureException,
// UnknownResponseException, AuthenticationFailureException {
// BoundRequestBuilder req = this.client.prepareGet(this.serviceUrl + "/" +
// container + "/" + object);
// if (null != option) for (Header h : option.toHeaders())
// req.addHeader(h.getName(), h.getValue());
// Response resp;
// try {
// resp = req.execute().get();
// } catch (InterruptedException e) {
// throw new OperationFailureException(e);
// } catch (ExecutionException e) {
// throw new OperationFailureException(e.getCause());
// }
// int c = this.handleStatusCode(resp, 200, 404);
// if (c == 404) throw new OperationFailureException("Swift fetch failure for
// given object not found.");
// if (c != 200) throw new OperationFailureException("Swift fetch failure with
// invalid response: " + resp.toString());
//
// try {
// return resp.getResponseBodyAsStream();
// } catch (IOException e) {
// throw new OperationFailureException("Inputstream create failure: " +
// resp.toString(), e);
// }
// }
//
// /**
// * TODO: large object upload with segments.
// *
// * Create/Update/Copy Object (PUT /account/container/object) <br>
// * Maybe Chunked transfer encoding<br>
// *
// * @throws OperationFailureException
// *
// * @throws UnknownResponseException
// * @throws AuthenticationFailureException
// */
// public void cp(InputStream fromObjectStream, String toContainer, String
// toObject) throws OperationFailureException,
// UnknownResponseException, AuthenticationFailureException {
// this.cp(fromObjectStream, toContainer, toObject, HTTP.OCTET_STREAM_TYPE,
// null);
// }
//
// public void cp(InputStream fromObjectStream, String toContainer, String
// toObject, String contentType)
// throws OperationFailureException, UnknownResponseException,
// AuthenticationFailureException {
// this.cp(fromObjectStream, toContainer, toObject, contentType, null);
// }
//
// public void cp(InputStream fromObjectStream, String toContainer, String
// toObject, Map<String, String> metadata)
// throws OperationFailureException, UnknownResponseException,
// AuthenticationFailureException {
// this.cp(fromObjectStream, toContainer, toObject, HTTP.OCTET_STREAM_TYPE,
// metadata);
// }
//
// public void cp(InputStream fromObjectStream, String toContainer, String
// toObject, String contentType,
// Map<String, String> metadata) throws OperationFailureException,
// UnknownResponseException,
// AuthenticationFailureException {
// String msg = " copying to swift as user [" + this.username + "], to
// destination container [" + toContainer
// + "] and object [" + toObject + "] in chunked mode.";
// logger.info("Begin" + msg);
// BoundRequestBuilder req = this.initcp(toContainer, toObject, metadata);
// req.setBody(fromObjectStream);
// req.addHeader(HttpHeaders.CONTENT_TYPE, contentType);
// req.addHeader(HttpHeaders.TRANSFER_ENCODING, "");
// entity.setChunked(true);
// req.setEntity(entity);
// try {
// this.docp(req);
// } finally {
// try {
// fromObjectStream.close();
// } catch (IOException e) {
// throw new OperationFailureException("Inputstream close failure: " +
// req.toString(), e);
// }
// }
// logger.info("End" + msg);
// }
//
// public void cp(InputStream fromObjectStream, String toContainer, String
// toObject, long bytes)
// throws OperationFailureException, UnknownResponseException,
// AuthenticationFailureException {
// this.cp(fromObjectStream, toContainer, toObject, bytes,
// HTTP.OCTET_STREAM_TYPE, null);
// }
//
// public void cp(InputStream fromObjectStream, String toContainer, String
// toObject, long bytes, String contentType)
// throws OperationFailureException, UnknownResponseException,
// AuthenticationFailureException {
// this.cp(fromObjectStream, toContainer, toObject, bytes, contentType, null);
// }
//
// public void cp(InputStream fromObjectStream, String toContainer, String
// toObject, long bytes, Map<String, String> metadata)
// throws OperationFailureException, UnknownResponseException,
// AuthenticationFailureException {
// this.cp(fromObjectStream, toContainer, toObject, bytes,
// HTTP.OCTET_STREAM_TYPE, metadata);
// }
//
// // TODO: now no MD5 checksum.
// public void cp(InputStream fromObjectStream, String toContainer, String
// toObject, long bytes, String contentType,
// Map<String, String> metadata) throws OperationFailureException,
// UnknownResponseException,
// AuthenticationFailureException {
// String msg = " copying to swift as user [" + this.username + "], to
// destination container [" + toContainer
// + "] and object [" + toObject + "] with fixed size [" + bytes + "].";
// logger.info("Begin" + msg);
// HttpPut req = this.initcp(toContainer, toObject, metadata);
// BasicHttpEntity entity = new BasicHttpEntity();
// entity.setContent(fromObjectStream);
// entity.setContentType(contentType);
// entity.setContentLength(bytes);
// req.setEntity(entity);
// try {
// this.docp(req);
// } finally {
// try {
// fromObjectStream.close();
// } catch (IOException e) {
// throw new OperationFailureException("Inputstream close failure: " +
// req.toString(), e);
// }
// }
// logger.info("End" + msg);
//
// // TODO: checksum
// // String checksumResp = resp.getFirstHeader("ETag").getValue();
// // if (null != checksumReq && !checksumReq.equals(checksumResp))
// // logger.warn("\tcpoying successfully, but checksum returned from swift is
// not correct: original ["
// // + checksumReq + "], returned [" + checksumResp + "].");
// }
//
// private BoundRequestBuilder initcp(String container, String object,
// Map<String, String> metadata) {
// BoundRequestBuilder req = this.client.preparePut(this.serviceUrl + "/" +
// container + "/" + object);
// if (null != metadata) {
// logger.debug("\tcpoying with metadata: " + metadata.toString());
// for (String name : metadata.keySet())
// req.addHeader(OBJECT_META_PREFIX + name, metadata.get(name));
// }
// return req;
// }
//
// private HttpResponse docp(HttpUriRequest req) throws
// OperationFailureException, UnknownResponseException,
// AuthenticationFailureException {
// HttpResponse resp = this.executeRequest(req);
// int c = this.handleStatusCode(resp, 201, 412, 422);
// if (c == 422) throw new OperationFailureException("Swift object upload
// failure for wrong checksum.");
// if (c == 412)
// throw new OperationFailureException("Swift object upload failure for missing
// Content-Type or Content_Length.");
// if (c != 201)
// throw new OperationFailureException("Swift object upload failure with invalid
// response: " + resp.toString());
// return resp;
// }
//
// /**
// * TODO: update the metadata of new object to be copied, maybe copy to self to
// change the Content-Type
// *
// * @param fromContainer
// * @param fromObject
// * @param toObject
// *
// * @throws OperationFailureException
// * @throws UnknownResponseException
// * @throws AuthenticationFailureException
// */
// public void cp(String fromContainer, String fromObject, String toContainer,
// String toObject)
// throws OperationFailureException, UnknownResponseException,
// AuthenticationFailureException {
// HttpPut req = new HttpPut(this.serviceUrl + "/" + toContainer + "/" +
// toObject);
// req.addHeader("X-Copy-From", fromContainer + "/" + fromObject);
// HttpResponse resp = this.executeRequest(req);
// int c = this.handleStatusCode(resp, 201, 412, 422);
// if (c == 422) throw new OperationFailureException("Swift object upload
// failure for wrong checksum.");
// if (c == 412)
// throw new OperationFailureException("Swift object upload failure for missing
// Content-Type or Content_Length.");
// if (c != 201)
// throw new OperationFailureException("Swift object upload failure with invalid
// response: " + resp.toString());
// }
//
// /**
// * Delete object (DELETE /account/container/object)
// *
// * @throws OperationFailureException
// * @throws UnknownResponseException
// * @throws AuthenticationFailureException
// */
// public void rm(String container, String object) throws
// OperationFailureException, UnknownResponseException,
// AuthenticationFailureException {
// HttpResponse resp = this.executeRequest(new HttpDelete(this.serviceUrl + "/"
// + container + "/" + object));
// int c = this.handleStatusCode(resp, 204, 404);
// if (c == 404) throw new OperationFailureException("Swift object remove
// failure for given object not found.");
// if (c != 204)
// throw new OperationFailureException("Swift object remove failure with invalid
// response: " + resp.toString());
// }
//
// // private routines
// private String getListResponse(String container, ListOption option) throws
// OperationFailureException,
// UnknownResponseException, AuthenticationFailureException {
// StringBuilder sb = new StringBuilder(this.serviceUrl);
// if (null != container) sb.append("/").append(container);
// if (null != option) {
// String qs = option.toString();
// if (qs.length() > 0) sb.append("?").append(qs);
// }
// HttpResponse resp = this.executeRequest(new HttpGet(sb.toString()));
// int c = this.handleStatusCode(resp, 200, 204, 404);
// if (c == 204) return null;
// if (c == 404)
// throw new OperationFailureException("Swift list failure for given parent
// (account/container) not found.");
// if (c != 200) throw new OperationFailureException("Swift list failure with
// invalid response: " + resp.toString());
// HttpEntity entity = resp.getEntity();
// if (null == entity) {
// try {
// EntityUtils.consume(entity);
// } catch (IOException e) {
// throw new UnknownResponseException("Invalid content of response: " +
// resp.toString());
// }
// throw new UnknownResponseException("Invalid content of response: " +
// resp.toString());
// }
// try {
// return EntityUtils.toString(entity);
// } catch (Exception e) {
// throw new UnknownResponseException("Invalid content of response: " +
// resp.toString());
// }
// }
//
// private class ResponseStatus {
// private int code;
// private String reasonPhrase;
//
// public ResponseStatus(int code, String reasonPhrase) {
// super();
// this.code = code;
// this.reasonPhrase = reasonPhrase;
// }
// }
//
// /**
// * @param response
// * @param ignoreCodes
// * http result codes handler by invoker, they should be ordered for binary
// search.
// * @return
// * @throws UnknownResponseException
// * @throws OperationFailureException
// * @throws AuthenticationFailureException
// */
// private int handleStatusCode(Response response, int... ignoreCodes) throws
// UnknownResponseException,
// OperationFailureException {
// ResponseStatus r = new ResponseStatus(response.getStatusCode(),
// response.getStatusText());
// int c = response.getStatusCode();
// if (c < 300) return c;
// if (null != ignoreCodes && ignoreCodes.length > 0 &&
// Arrays.binarySearch(ignoreCodes, c) >= 0) return c;
// throw new OperationFailureException(response.getStatusText());
// }
//
// private HttpResponse executeRequest(HttpUriRequest req) throws
// OperationFailureException, AuthenticationFailureException,
// UnknownResponseException {
// return this.executeRequest(req, true);
// }
//
// private HttpResponse executeRequest(HttpUriRequest req, boolean reauth)
// throws OperationFailureException,
// AuthenticationFailureException, UnknownResponseException {
// if (null != this.token) req.addHeader(AUTH_TOKEN_HEADER_NAME, this.token);
//
// // HttpClient client = HttpClientFactory.getSharedClient();
// HttpClient client = HttpClientFactory.createSingleHttpClient();
// try {
// HttpResponse response = client.execute(req);
// // try to auth again on timeout.
// if (this.fetchStatus(response).code == 401) {
// if (!reauth) throw new AuthenticationFailureException();
// this.login();
// response = client.execute(req);
// }
// return response;
// } catch (IOException e) {
// throw new OperationFailureException("Swift request executing failure", e);
// } finally {
// // HttpClientFactory.cleanup();
// // client.getConnectionManager().shutdown();
// }
// }
//
// public static class ListOption implements Serializable {
// private static final long serialVersionUID = 4139246530469253002L;
//
// public enum Format {
// json, xml;
// private final static Format DEFAULT_DETAIL_FORMAT = Format.json;
// }
//
// private int limit;
// private String marker;
// private String prefix;
// private String path;
// private String delimiter;
// private Format format;
//
// public ListOption() {
// this.format = Format.DEFAULT_DETAIL_FORMAT;
// }
//
// public ListOption(int limit, String marker, Format format) {
// super();
// this.limit = limit;
// this.marker = marker;
// this.format = format;
// }
//
// public String toString() {
// StringBuilder sb = new StringBuilder();
// if (null != format) sb.append("&format=").append(format.name());
// if (limit > 0) sb.append("&limit=").append(limit);
// if (null != marker) sb.append("&marker=").append(marker);
// if (null != prefix) sb.append("&prefix=").append(prefix);
// if (null != path) sb.append("&path=").append(path);
// if (null != delimiter) sb.append("&delimiter=").append(delimiter);
// if (sb.length() > 0) sb.deleteCharAt(0);
// return sb.toString();
// }
//
// public void setFormat(Format format) {
// this.format = format;
// }
//
// public void setLimit(int limit) {
// this.limit = limit;
// }
//
// public void setMarker(String marker) {
// this.marker = marker;
// }
//
// public void setPrefix(String prefix) {
// this.prefix = prefix;
// }
//
// public void setPath(String path) {
// this.path = path;
// }
//
// public void setDelimiter(String delimiter) {
// this.delimiter = delimiter;
// }
// }
//
// public static class FetchOption {
// public static final FetchOption DEFAULT_FETCH_OPTION = new FetchOption();
// private static final String IF_MATCH_HEADER_NAME = "If-Match";
// private static final String IF_NONE_MATCH_HEADER_NAME = "If-None-Match";
// private static final String IF_MODIFIED_SINCE_HEADER_NAME =
// "If-Modified-Since";
// private static final String IF_UNMODIFIED_SINCE_HEADER_NAME =
// "If-Unmodified-Since";
// private static final String RANGE_HEADER_NAME = "Range";
// private static final String RANGE_VALUE_PREFIX = "bytes=";
// private String[] match;
// private String[] noneMatch;
// private Date modifiedSince;
// private Date unmodifiedSince;
// private int[] range;
//
// public void setRange(int... range) {
// if (null != range && range.length == 0 || range.length > 2)
// throw new RuntimeException("A range should containe one or two integer.");
// this.range = range;
// }
//
// public void setMatch(String[] match) {
// this.match = match;
// }
//
// public void setNoneMatch(String[] noneMatch) {
// this.noneMatch = noneMatch;
// }
//
// public void setModifiedSince(Date modifiedSince) {
// this.modifiedSince = modifiedSince;
// }
//
// public void setUnmodifiedSince(Date unmodifiedSince) {
// this.unmodifiedSince = unmodifiedSince;
// }
//
// private FastDateFormat format;
// public final Header[] toHeaders() {
// List<Header> r = new ArrayList<Header>();
// // •Range: bytes=-5 - last five bytes of the object
// // •Range: bytes=10-15 - the five bytes after a 10-byte offset
// // •Range: bytes=32- - all data after the first 32 bytes of the
// // object
// if (range != null)
// switch (range.length) {
// case 1:
// if (range[0] < 0) r.add(new BasicHeader(RANGE_HEADER_NAME, RANGE_VALUE_PREFIX
// + range[0]));
// else if (range[0] > 0) r.add(new BasicHeader(RANGE_HEADER_NAME,
// RANGE_VALUE_PREFIX + range[0] + "-"));
// break;
// case 2:
// if (range[0] > 0 && range[1] > 0 && range[1] > range[0])
// r.add(new BasicHeader(RANGE_HEADER_NAME, RANGE_VALUE_PREFIX + range[0] + "-"
// + range[1]));
// break;
// default:
// break;
// }
// if (this.match != null && this.match.length > 0)
// r.add(new BasicHeader(IF_MATCH_HEADER_NAME, "\"" + Texts.join(',',
// this.match) + "\""));
// if (this.noneMatch != null && this.noneMatch.length > 0)
// r.add(new BasicHeader(IF_NONE_MATCH_HEADER_NAME, "\"" + Texts.join(',',
// this.noneMatch) + "\""));
// if (this.modifiedSince != null)
// r.add(new BasicHeader(IF_MODIFIED_SINCE_HEADER_NAME,
// DateUtils.httpFormat.format(this.modifiedSince)));
// if (this.unmodifiedSince != null)
// r.add(new BasicHeader(IF_UNMODIFIED_SINCE_HEADER_NAME,
// DateUtils.httpFormat.format(this.unmodifiedSince)));
// return r.toArray(new Header[r.size()]);
// }
// }
// }